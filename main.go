package main

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
	"truedown/internal/api"
	"truedown/internal/downloader"
)

//go:embed web
var webFS embed.FS

// exeDir returns the directory that contains the running executable,
// so paths like aria2c.exe are resolved correctly regardless of cwd.
func exeDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(exe)
}

func main() {
	base := exeDir()
	aria2 := filepath.Join(base, "aria2c.exe")
	if _, err := os.Stat(aria2); os.IsNotExist(err) {
		aria2 = filepath.Join("aria2", "aria2c.exe")
	}
	aria2, err := filepath.Abs(aria2)
	if err != nil {
		log.Fatal(err)
	}
	dataDir := os.Getenv("TRUEDOWN_DATA_DIR")
	if dataDir == "" {
		dataDir = base
	}
	downloads := filepath.Join(dataDir, "downloads")
	database := filepath.Join(dataDir, "truedown.db")
	dm, err := downloader.NewManager(aria2, downloads, database)
	if err != nil {
		log.Fatal(err)
	}
	if err := dm.Start(); err != nil {
		dm.Stop()
		log.Fatal(err)
	}
	defer dm.Stop()

	mux := http.NewServeMux()
	api.Register(mux, dm)

	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))

	addr := os.Getenv("TRUEDOWN_ADDR")
	if addr == "" {
		addr = ":15151"
	}
	browserURL := "http://" + addr
	if strings.HasPrefix(addr, ":") {
		browserURL = "http://localhost" + addr
	}
	log.Printf("TrueDown listening on %s", addr)
	if os.Getenv("TRUEDOWN_NO_BROWSER") == "" {
		go openBrowser(browserURL)
	}
	server := &http.Server{Addr: addr, Handler: mux}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server: %v", err)
		}
	case <-stop:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("HTTP shutdown: %v", err)
		}
	}
}

func openBrowser(url string) {
	time.Sleep(300 * time.Millisecond)
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	cmd.Start()
}
