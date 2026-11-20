package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strings"

	"github.com/CAFxX/sqlgen"
)

var exitFunc = os.Exit

func main() {
	exitFunc(run(os.Args[1:], os.Stdout, os.Stderr))
}

// versionString reports the module version the binary was built from ("go install ...@v0.3.1"), or
// "(devel)" for local builds and when no build information is available.
func versionString(info *debug.BuildInfo) string {
	if info == nil || info.Main.Version == "" {
		return "sqlgen (devel)"
	}
	return "sqlgen " + info.Main.Version
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sqlgen", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		cfgPath     string
		cfgExplicit bool
		outPath     string
		toStdout    bool
		checkMode   bool
		showVersion bool
	)

	fs.Func("config", "path to sqlgen YAML configuration file (default \"sqlgen.yaml\")", func(val string) error {
		cfgPath = val
		cfgExplicit = true
		return nil
	})
	fs.StringVar(&outPath, "out", "", "optional override for generated Go output file path")
	fs.BoolVar(&toStdout, "stdout", false, "write generated Go code to stdout instead of disk")
	fs.BoolVar(&checkMode, "check", false, "CI verification mode: compare generated code against disk without writing")
	fs.BoolVar(&showVersion, "version", false, "print sqlgen version and exit")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	if showVersion {
		info, _ := debug.ReadBuildInfo()
		_, _ = fmt.Fprintln(stdout, versionString(info))
		return 0
	}

	if !cfgExplicit {
		if fs.NArg() > 1 {
			_, _ = fmt.Fprintf(stderr, "sqlgen: expected at most 1 positional config argument, got %d\n", fs.NArg())
			return 2
		}
		if fs.NArg() == 1 {
			cfgPath = fs.Arg(0)
		} else {
			cfgPath = "sqlgen.yaml"
		}
	} else if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "sqlgen: unexpected positional arguments alongside -config\n")
		return 2
	}

	if toStdout && checkMode {
		_, _ = fmt.Fprintf(stderr, "sqlgen: -stdout and -check are mutually exclusive\n")
		return 2
	}

	if !toStdout && !checkMode {
		if err := sqlgen.GenerateFromYAMLFile(cfgPath, outPath); err != nil {
			reportErr(stderr, err)
			return 1
		}
		return 0
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "sqlgen: read %s: %v\n", cfgPath, err)
		return 1
	}
	cfg, err := sqlgen.ParseYAML(data)
	if err != nil {
		reportErr(stderr, err)
		return 1
	}

	src, err := sqlgen.Generate(cfg)
	if err != nil {
		reportErr(stderr, err)
		return 1
	}

	if toStdout {
		if _, err := stdout.Write(src); err != nil {
			_, _ = fmt.Fprintf(stderr, "sqlgen: write stdout: %v\n", err)
			return 1
		}
		return 0
	}

	targetOut, err := sqlgen.OutputPath(cfgPath, cfg, outPath)
	if err != nil {
		reportErr(stderr, err)
		return 1
	}
	existing, err := os.ReadFile(targetOut)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "sqlgen: -check failed: cannot read %s: %v (run sqlgen -config %s to regenerate)\n", targetOut, err, cfgPath)
		return 2
	}
	if !bytes.Equal(existing, src) {
		_, _ = fmt.Fprintf(stderr, "sqlgen: -check failed: %s is out of date with %s (run sqlgen -config %s to regenerate)\n", targetOut, cfgPath, cfgPath)
		return 2
	}
	return 0
}

// reportErr prints err prefixed with "sqlgen: ", unless the library already did.
func reportErr(w io.Writer, err error) {
	msg := err.Error()
	if !strings.HasPrefix(msg, "sqlgen:") {
		msg = "sqlgen: " + msg
	}
	_, _ = fmt.Fprintln(w, msg)
}
