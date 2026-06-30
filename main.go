package main

import (
	"embed"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
	downloads := filepath.Join(base, "downloads")
	dm := downloader.NewManager(aria2, downloads)
	dm.Start()
	defer dm.Stop()

	mux := http.NewServeMux()
	api.Register(mux, dm)

	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))

	log.Println("TrueDown listening on :15151")
	go openBrowser("http://localhost:15151")
	if err := http.ListenAndServe(":15151", mux); err != nil {
		log.Fatal(err)
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
