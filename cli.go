package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	mycel "github.com/myceldb/mycel-go-sdk"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/common/v1"
	"golang.org/x/term"
)

const keychainService = "mycel-mcp"

type persistedConfig struct {
	DaemonAddr      string `json:"daemon_addr,omitempty"`
	Username        string `json:"username,omitempty"`
	AuthSessionID   string `json:"auth_session_id,omitempty"`
	DefaultSpaceID  string `json:"default_space_id,omitempty"`
	DefaultDomainID string `json:"default_domain_id,omitempty"`
	MaxResults      int32  `json:"max_results,omitempty"`
	WriteEnabled    bool   `json:"write_enabled,omitempty"`
	AdminEnabled    bool   `json:"admin_enabled,omitempty"`
}

type credentialStore interface {
	Get(username string) (string, bool, error)
	Set(username, refreshToken string) error
	Delete(username string) error
	Description() string
}

func runCLI(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return runServe(stdin, stdout)
	}
	switch args[0] {
	case "serve":
		return runServe(stdin, stdout)
	case "login":
		return runLogin(args[1:], stdin, stdout)
	case "logout":
		return runLogout(args[1:], stdout)
	case "doctor":
		return runDoctor(args[1:], stdout)
	case "configure", "config":
		return runConfigure(args[1:], stdin, stdout)
	case "help", "--help", "-h":
		printUsage(stdout)
		return nil
	case "version", "--version", "-v":
		fmt.Fprintf(stdout, "%s %s\n", serverName, serverVersion)
		return nil
	default:
		printUsage(stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintf(w, `%s %s

Usage:
  mycel-mcp serve       Run the stdio MCP server (default)
  mycel-mcp login       Log in to myceld and store a refresh token securely
  mycel-mcp logout      Revoke/remove stored MCP credentials
  mycel-mcp configure   Choose default Mycel space/domain for MCP tools
  mycel-mcp doctor      Diagnose local MCP configuration and daemon access
  mycel-mcp version     Print version

Environment variables still override local config when serving.
`, serverName, serverVersion)
}

func runServe(stdin io.Reader, stdout io.Writer) error {
	a := newAppFromEnv()
	defer func() { _ = a.close() }()
	return newServer(a).serve(stdin, stdout)
}

func runLogin(args []string, stdin io.Reader, stdout io.Writer) error {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	fs.SetOutput(stdout)
	var daemonAddr, username, password string
	fs.StringVar(&daemonAddr, "daemon-addr", "", "myceld gRPC address")
	fs.StringVar(&username, "username", "", "Mycel username")
	fs.StringVar(&password, "password", "", "Mycel password; if omitted, prompted without echo on terminals")
	if err := fs.Parse(args); err != nil {
		return err
	}

	local, path, err := loadPersistedConfig()
	if err != nil {
		return err
	}
	reader := bufio.NewReader(stdin)
	if strings.TrimSpace(daemonAddr) == "" {
		def := firstNonEmpty(strings.TrimSpace(os.Getenv("MYCELD_GRPC_ADDR")), local.DaemonAddr, mycel.DefaultAddr)
		value, err := promptLine(reader, stdout, "Daemon address", def)
		if err != nil {
			return err
		}
		daemonAddr = value
	}
	if strings.TrimSpace(username) == "" {
		def := firstNonEmpty(strings.TrimSpace(os.Getenv("MYCEL_USERNAME")), local.Username)
		value, err := promptLine(reader, stdout, "Username", def)
		if err != nil {
			return err
		}
		username = value
	}
	if strings.TrimSpace(password) == "" {
		value, err := promptPassword(reader, stdout, "Password")
		if err != nil {
			return err
		}
		password = value
	}
	if strings.TrimSpace(daemonAddr) == "" {
		return errors.New("daemon address is required")
	}
	if strings.TrimSpace(username) == "" {
		return errors.New("username is required")
	}
	if password == "" {
		return errors.New("password is required")
	}

	cfg := mycel.ConfigFromEnv()
	cfg.Addr = strings.TrimSpace(daemonAddr)
	cfg.Username = ""
	cfg.Password = ""
	applyMCPClientInfo(&cfg)
	client, err := mycel.Dial(context.Background(), cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	res, err := client.Login(context.Background(), strings.TrimSpace(username), password)
	if err != nil {
		return err
	}
	refreshToken := strings.TrimSpace(res.GetRefreshToken())
	if refreshToken == "" {
		return errors.New("login succeeded but daemon did not return a refresh token")
	}
	store := defaultCredentialStore(filepath.Dir(path))
	if err := store.Set(strings.TrimSpace(username), refreshToken); err != nil {
		return err
	}
	local.DaemonAddr = strings.TrimSpace(daemonAddr)
	local.Username = strings.TrimSpace(username)
	local.AuthSessionID = strings.TrimSpace(res.GetAuthSessionId())
	if local.MaxResults <= 0 {
		local.MaxResults = defaultMaxResults
	}
	if err := savePersistedConfig(path, local); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "logged in as %s\n", res.GetPrincipal().GetUsername())
	fmt.Fprintf(stdout, "config: %s\n", path)
	fmt.Fprintf(stdout, "credentials: %s\n", store.Description())
	if local.DefaultSpaceID == "" || local.DefaultDomainID == "" {
		fmt.Fprintln(stdout, "next: run `mycel-mcp configure` to choose default space/domain")
	}
	return nil
}

func runLogout(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("logout", flag.ContinueOnError)
	fs.SetOutput(stdout)
	var keepConfig bool
	fs.BoolVar(&keepConfig, "keep-config", false, "keep non-secret config values such as default space/domain")
	if err := fs.Parse(args); err != nil {
		return err
	}
	local, path, err := loadPersistedConfig()
	if err != nil {
		return err
	}
	username := firstNonEmpty(strings.TrimSpace(os.Getenv("MYCEL_USERNAME")), local.Username)
	store := defaultCredentialStore(filepath.Dir(path))
	if username != "" {
		if token, ok, err := store.Get(username); err != nil {
			return err
		} else if ok && token != "" {
			cfg := mycel.ConfigFromEnv()
			if strings.TrimSpace(cfg.Addr) == "" || cfg.Addr == mycel.DefaultAddr {
				cfg.Addr = firstNonEmpty(local.DaemonAddr, mycel.DefaultAddr)
			}
			cfg.Username = ""
			cfg.Password = ""
			cfg.AccessToken = ""
			cfg.RefreshToken = token
			applyMCPClientInfo(&cfg)
			client, err := mycel.Dial(context.Background(), cfg)
			if err == nil {
				if _, refreshErr := client.Refresh(context.Background(), token); refreshErr == nil {
					_, _ = client.Logout(context.Background(), "")
				}
				_ = client.Close()
			}
		}
		if err := store.Delete(username); err != nil {
			return err
		}
	}
	local.Username = ""
	local.AuthSessionID = ""
	if !keepConfig {
		local.DefaultSpaceID = ""
		local.DefaultDomainID = ""
	}
	if err := savePersistedConfig(path, local); err != nil {
		return err
	}
	fmt.Fprintln(stdout, "logged out")
	return nil
}

func runConfigure(args []string, stdin io.Reader, stdout io.Writer) error {
	fs := flag.NewFlagSet("configure", flag.ContinueOnError)
	fs.SetOutput(stdout)
	var spaceID, domainID string
	var maxResults int
	var enableWrites, disableWrites bool
	fs.StringVar(&spaceID, "space-id", "", "default space ID")
	fs.StringVar(&domainID, "domain-id", "", "default domain ID")
	fs.IntVar(&maxResults, "max-results", 0, "default/max MCP page size")
	fs.BoolVar(&enableWrites, "enable-writes", false, "enable mutating MCP graph tools")
	fs.BoolVar(&disableWrites, "disable-writes", false, "disable mutating MCP graph tools")
	if err := fs.Parse(args); err != nil {
		return err
	}
	local, path, err := loadPersistedConfig()
	if err != nil {
		return err
	}
	if enableWrites && disableWrites {
		return errors.New("--enable-writes and --disable-writes are mutually exclusive")
	}
	changedOnly := false
	if maxResults > 0 {
		local.MaxResults = normalizeMaxResults(int32(maxResults))
		changedOnly = true
	}
	if enableWrites {
		local.WriteEnabled = true
		changedOnly = true
	}
	if disableWrites {
		local.WriteEnabled = false
		changedOnly = true
	}
	if strings.TrimSpace(spaceID) != "" && strings.TrimSpace(domainID) != "" {
		local.DefaultSpaceID = strings.TrimSpace(spaceID)
		local.DefaultDomainID = strings.TrimSpace(domainID)
		if err := savePersistedConfig(path, local); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "configured default space/domain\nconfig: %s\n", path)
		return nil
	}
	if changedOnly {
		if err := savePersistedConfig(path, local); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "updated config: %s\n", path)
		fmt.Fprintf(stdout, "write_enabled: %t\n", local.WriteEnabled)
		return nil
	}

	a, err := newAppFromEnvWithError()
	if err != nil {
		return err
	}
	defer func() { _ = a.close() }()
	client, err := a.mycelClient(context.Background())
	if err != nil {
		return err
	}
	callCtx, cancel := client.AuthCallContext(context.Background())
	spaces, err := client.Space.ListSpaces(callCtx, &clientv1.ListSpacesRequest{PageSize: hardMaxResults})
	cancel()
	if err != nil {
		return err
	}
	if len(spaces.GetSpaces()) == 0 {
		return errors.New("no visible spaces; create a space first")
	}
	reader := bufio.NewReader(stdin)
	space, err := chooseSpace(reader, stdout, spaces.GetSpaces(), local.DefaultSpaceID)
	if err != nil {
		return err
	}
	callCtx, cancel = client.AuthCallContext(context.Background())
	domains, err := client.Domain.ListDomains(callCtx, &clientv1.ListDomainsRequest{SpaceId: space.GetSpaceId(), PageSize: hardMaxResults})
	cancel()
	if err != nil {
		return err
	}
	if len(domains.GetDomains()) == 0 {
		return fmt.Errorf("space %s has no visible domains", space.GetSpaceId())
	}
	domain, err := chooseDomain(reader, stdout, domains.GetDomains(), local.DefaultDomainID)
	if err != nil {
		return err
	}
	local.DefaultSpaceID = space.GetSpaceId()
	local.DefaultDomainID = domain.GetDomainId()
	if local.DaemonAddr == "" {
		local.DaemonAddr = a.mycelCfg.Addr
	}
	if local.Username == "" {
		local.Username = a.credentialUsername
	}
	if local.MaxResults <= 0 {
		local.MaxResults = defaultMaxResults
	}
	if err := savePersistedConfig(path, local); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "configured default space %s (%s) domain %s (%s)\n", space.GetName(), space.GetSpaceId(), domain.GetKey(), domain.GetDomainId())
	fmt.Fprintf(stdout, "config: %s\n", path)
	return nil
}

func runDoctor(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(stdout)
	if err := fs.Parse(args); err != nil {
		return err
	}
	local, path, err := loadPersistedConfig()
	if err != nil {
		return err
	}
	store := defaultCredentialStore(filepath.Dir(path))
	failures := 0
	check := func(name string, err error) {
		if err != nil {
			failures++
			fmt.Fprintf(stdout, "[FAIL] %s: %v\n", name, err)
			return
		}
		fmt.Fprintf(stdout, "[ OK ] %s\n", name)
	}
	fmt.Fprintf(stdout, "%s %s\n", serverName, serverVersion)
	fmt.Fprintf(stdout, "config path: %s\n", path)
	fmt.Fprintf(stdout, "credential store: %s\n", store.Description())
	check("config readable", nil)
	if local.DaemonAddr != "" {
		fmt.Fprintf(stdout, "daemon addr: %s\n", local.DaemonAddr)
	}
	username := firstNonEmpty(strings.TrimSpace(os.Getenv("MYCEL_USERNAME")), local.Username)
	if username == "" && strings.TrimSpace(os.Getenv("MYCEL_REFRESH_TOKEN")) == "" && strings.TrimSpace(os.Getenv("MYCEL_PASSWORD")) == "" {
		check("credentials present", errors.New("not logged in; run `mycel-mcp login` or set MYCEL_USERNAME/MYCEL_PASSWORD"))
	} else if strings.TrimSpace(os.Getenv("MYCEL_PASSWORD")) != "" || strings.TrimSpace(os.Getenv("MYCEL_REFRESH_TOKEN")) != "" {
		check("credentials present", nil)
	} else {
		_, ok, err := store.Get(username)
		if err != nil {
			check("credentials present", err)
		} else if !ok {
			check("credentials present", fmt.Errorf("no stored refresh token for %q; run `mycel-mcp login`", username))
		} else {
			check("credentials present", nil)
		}
	}
	a, err := newAppFromEnvWithError()
	if err != nil {
		check("load runtime config", err)
	} else {
		defer func() { _ = a.close() }()
		client, err := a.mycelClient(context.Background())
		check("connect/authenticate", err)
		if err == nil {
			callCtx, cancel := client.AuthCallContext(context.Background())
			who, err := client.Auth.WhoAmI(callCtx, &commonv1.WhoAmIRequest{})
			cancel()
			check("whoami", err)
			if err == nil {
				fmt.Fprintf(stdout, "principal: %s (%s)\n", who.GetPrincipal().GetUsername(), who.GetPrincipal().GetPrincipalId())
			}
			if a.cfg.DefaultSpaceID != "" {
				callCtx, cancel = client.AuthCallContext(context.Background())
				_, err = client.Space.GetSpace(callCtx, &clientv1.GetSpaceRequest{SpaceId: a.cfg.DefaultSpaceID})
				cancel()
				check("default space accessible", err)
			} else {
				check("default space configured", errors.New("run `mycel-mcp configure`"))
			}
			if a.cfg.DefaultSpaceID != "" && a.cfg.DefaultDomainID != "" {
				callCtx, cancel = client.AuthCallContext(context.Background())
				_, err = client.Domain.GetDomain(callCtx, &clientv1.GetDomainRequest{SpaceId: a.cfg.DefaultSpaceID, DomainId: a.cfg.DefaultDomainID})
				cancel()
				check("default domain accessible", err)
			} else {
				check("default domain configured", errors.New("run `mycel-mcp configure`"))
			}
		}
	}
	if failures > 0 {
		return fmt.Errorf("doctor found %d problem(s)", failures)
	}
	return nil
}

func chooseSpace(reader *bufio.Reader, stdout io.Writer, spaces []*clientv1.Space, current string) (*clientv1.Space, error) {
	fmt.Fprintln(stdout, "Visible spaces:")
	def := 1
	for i, space := range spaces {
		marker := ""
		if space.GetSpaceId() == current {
			marker = " *"
			def = i + 1
		}
		fmt.Fprintf(stdout, "  %d. %s (%s)%s\n", i+1, space.GetName(), space.GetSpaceId(), marker)
	}
	choice, err := promptLine(reader, stdout, "Choose default space", strconv.Itoa(def))
	if err != nil {
		return nil, err
	}
	idx, err := strconv.Atoi(strings.TrimSpace(choice))
	if err != nil || idx < 1 || idx > len(spaces) {
		return nil, errors.New("invalid space selection")
	}
	return spaces[idx-1], nil
}

func chooseDomain(reader *bufio.Reader, stdout io.Writer, domains []*clientv1.Domain, current string) (*clientv1.Domain, error) {
	fmt.Fprintln(stdout, "Visible domains:")
	def := 1
	for i, domain := range domains {
		marker := ""
		if domain.GetDomainId() == current || (current == "" && domain.GetDefault()) {
			marker = " *"
			def = i + 1
		}
		fmt.Fprintf(stdout, "  %d. %s/%s (%s)%s\n", i+1, domain.GetKey(), domain.GetName(), domain.GetDomainId(), marker)
	}
	choice, err := promptLine(reader, stdout, "Choose default domain", strconv.Itoa(def))
	if err != nil {
		return nil, err
	}
	idx, err := strconv.Atoi(strings.TrimSpace(choice))
	if err != nil || idx < 1 || idx > len(domains) {
		return nil, errors.New("invalid domain selection")
	}
	return domains[idx-1], nil
}

func promptLine(reader *bufio.Reader, stdout io.Writer, label, def string) (string, error) {
	if def != "" {
		fmt.Fprintf(stdout, "%s [%s]: ", label, def)
	} else {
		fmt.Fprintf(stdout, "%s: ", label)
	}
	value, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	value = strings.TrimSpace(value)
	if value == "" {
		value = def
	}
	return value, nil
}

func promptPassword(reader *bufio.Reader, stdout io.Writer, label string) (string, error) {
	fmt.Fprintf(stdout, "%s: ", label)
	if term.IsTerminal(int(os.Stdin.Fd())) {
		b, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(stdout)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
	value, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return strings.TrimRight(value, "\r\n"), nil
}

func newAppFromEnvWithError() (*app, error) {
	local, path, err := loadPersistedConfig()
	if err != nil {
		return nil, err
	}
	store := defaultCredentialStore(filepath.Dir(path))
	mycelCfg := mycel.ConfigFromEnv()
	if strings.TrimSpace(os.Getenv("MYCELD_GRPC_ADDR")) == "" && strings.TrimSpace(local.DaemonAddr) != "" {
		mycelCfg.Addr = strings.TrimSpace(local.DaemonAddr)
	}
	envUsername := strings.TrimSpace(os.Getenv("MYCEL_USERNAME"))
	envPassword := os.Getenv("MYCEL_PASSWORD")
	envRefreshToken := strings.TrimSpace(os.Getenv("MYCEL_REFRESH_TOKEN"))
	credentialUsername := firstNonEmpty(envUsername, local.Username)
	storedRefreshToken := ""
	persistCredentials := false
	if envRefreshToken == "" && strings.TrimSpace(envPassword) == "" && credentialUsername != "" {
		if token, ok, err := store.Get(credentialUsername); err != nil {
			return nil, err
		} else if ok {
			storedRefreshToken = strings.TrimSpace(token)
			mycelCfg.RefreshToken = storedRefreshToken
			persistCredentials = storedRefreshToken != ""
		}
	}
	// The Go SDK auto-logins whenever Username is non-empty. For stored or
	// env-supplied refresh-token auth, clear Username/Password before Dial so the
	// client refreshes the durable session instead of trying password login with
	// an empty password.
	if strings.TrimSpace(mycelCfg.RefreshToken) != "" && strings.TrimSpace(envPassword) == "" {
		mycelCfg.Username = ""
		mycelCfg.Password = ""
	} else if envUsername == "" && strings.TrimSpace(local.Username) != "" && strings.TrimSpace(envPassword) != "" {
		mycelCfg.Username = strings.TrimSpace(local.Username)
	}
	applyMCPClientInfo(&mycelCfg)
	return &app{
		cfg:                mergeRuntimeConfig(local),
		mycelCfg:           mycelCfg,
		configPath:         path,
		localConfig:        local,
		store:              store,
		credentialUsername: credentialUsername,
		storedRefreshToken: storedRefreshToken,
		persistCredentials: persistCredentials,
	}, nil
}

func defaultMycelConfig() mycel.Config {
	cfg := mycel.ConfigFromEnv()
	applyMCPClientInfo(&cfg)
	return cfg
}

func applyMCPClientInfo(cfg *mycel.Config) {
	if strings.TrimSpace(cfg.ClientName) == "" || cfg.ClientName == "mycel-go-sdk" {
		cfg.ClientName = serverName
	}
	if strings.TrimSpace(cfg.Platform) == "" || cfg.Platform == "go" {
		cfg.Platform = "mcp"
	}
	if strings.TrimSpace(cfg.DeviceLabel) == "" {
		cfg.DeviceLabel = serverName
	}
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = defaultCallTimeout
	}
}

func mergeRuntimeConfig(local persistedConfig) config {
	maxResults := local.MaxResults
	if maxResults <= 0 {
		maxResults = defaultMaxResults
	}
	if value, ok := os.LookupEnv("MYCEL_MCP_MAX_RESULTS"); ok {
		maxResults = parseInt32(value, maxResults)
	}
	maxResults = normalizeMaxResults(maxResults)
	cfg := config{
		DefaultSpaceID:  strings.TrimSpace(local.DefaultSpaceID),
		DefaultDomainID: strings.TrimSpace(local.DefaultDomainID),
		MaxResults:      maxResults,
		WriteEnabled:    local.WriteEnabled,
		AdminEnabled:    local.AdminEnabled,
	}
	if value, ok := os.LookupEnv("MYCEL_MCP_DEFAULT_SPACE_ID"); ok {
		cfg.DefaultSpaceID = strings.TrimSpace(value)
	}
	if value, ok := os.LookupEnv("MYCEL_MCP_DEFAULT_DOMAIN_ID"); ok {
		cfg.DefaultDomainID = strings.TrimSpace(value)
	}
	if value, ok := os.LookupEnv("MYCEL_MCP_WRITE_ENABLED"); ok {
		cfg.WriteEnabled = parseBoolString(value)
	}
	if value, ok := os.LookupEnv("MYCEL_MCP_ADMIN_ENABLED"); ok {
		cfg.AdminEnabled = parseBoolString(value)
	}
	return cfg
}

func normalizeMaxResults(value int32) int32 {
	if value <= 0 {
		value = defaultMaxResults
	}
	if value > hardMaxResults {
		value = hardMaxResults
	}
	return value
}

func parseInt32(value string, fallback int32) int32 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
	if err != nil {
		return fallback
	}
	return int32(parsed)
}

func parseBoolString(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	default:
		return false
	}
}

func loadPersistedConfig() (persistedConfig, string, error) {
	path, err := configPath()
	if err != nil {
		return persistedConfig{}, "", err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return persistedConfig{}, path, nil
	}
	if err != nil {
		return persistedConfig{}, path, err
	}
	var cfg persistedConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return persistedConfig{}, path, fmt.Errorf("read %s: %w", path, err)
	}
	return cfg, path, nil
}

func savePersistedConfig(path string, cfg persistedConfig) error {
	if cfg.MaxResults <= 0 {
		cfg.MaxResults = defaultMaxResults
	}
	cfg.MaxResults = normalizeMaxResults(cfg.MaxResults)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func configPath() (string, error) {
	if value := strings.TrimSpace(os.Getenv("MYCEL_MCP_CONFIG")); value != "" {
		return value, nil
	}
	base := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME"))
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		base = filepath.Join(home, ".config")
	}
	return filepath.Join(base, "mycel-mcp", "config.json"), nil
}

func defaultCredentialStore(configDir string) credentialStore {
	if runtime.GOOS == "darwin" {
		return keychainCredentialStore{}
	}
	return fileCredentialStore{path: filepath.Join(configDir, "credentials.json")}
}

type keychainCredentialStore struct{}

func (keychainCredentialStore) Description() string {
	return "macOS Keychain service " + keychainService
}

func (keychainCredentialStore) Get(username string) (string, bool, error) {
	if strings.TrimSpace(username) == "" {
		return "", false, nil
	}
	cmd := exec.Command("security", "find-generic-password", "-s", keychainService, "-a", strings.TrimSpace(username), "-w")
	out, err := cmd.Output()
	if err != nil {
		return "", false, nil
	}
	return strings.TrimSpace(string(out)), true, nil
}

func (keychainCredentialStore) Set(username, refreshToken string) error {
	if strings.TrimSpace(username) == "" || strings.TrimSpace(refreshToken) == "" {
		return errors.New("username and refresh token are required")
	}
	cmd := exec.Command("security", "add-generic-password", "-s", keychainService, "-a", strings.TrimSpace(username), "-w", strings.TrimSpace(refreshToken), "-U")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("store refresh token in Keychain: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (keychainCredentialStore) Delete(username string) error {
	if strings.TrimSpace(username) == "" {
		return nil
	}
	cmd := exec.Command("security", "delete-generic-password", "-s", keychainService, "-a", strings.TrimSpace(username))
	if err := cmd.Run(); err != nil {
		return nil
	}
	return nil
}

type fileCredentialStore struct {
	path string
}

func (s fileCredentialStore) Description() string { return "file " + s.path }

func (s fileCredentialStore) Get(username string) (string, bool, error) {
	all, err := s.readAll()
	if err != nil {
		return "", false, err
	}
	token := strings.TrimSpace(all[strings.TrimSpace(username)])
	return token, token != "", nil
}

func (s fileCredentialStore) Set(username, refreshToken string) error {
	all, err := s.readAll()
	if err != nil {
		return err
	}
	all[strings.TrimSpace(username)] = strings.TrimSpace(refreshToken)
	return s.writeAll(all)
}

func (s fileCredentialStore) Delete(username string) error {
	all, err := s.readAll()
	if err != nil {
		return err
	}
	delete(all, strings.TrimSpace(username))
	return s.writeAll(all)
}

func (s fileCredentialStore) readAll() (map[string]string, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Credentials map[string]string `json:"credentials"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Credentials == nil {
		wrapper.Credentials = map[string]string{}
	}
	return wrapper.Credentials, nil
}

func (s fileCredentialStore) writeAll(credentials map[string]string) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	wrapper := struct {
		Credentials map[string]string `json:"credentials"`
	}{Credentials: credentials}
	data, err := json.MarshalIndent(wrapper, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
