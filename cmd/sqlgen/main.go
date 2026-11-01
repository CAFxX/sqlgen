package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/CAFxX/sqlgen"
)

func main() {
	cfgPath := flag.String("config", "sqlgen.yaml", "path to sqlgen YAML configuration file")
	outPath := flag.String("out", "", "optional override for generated Go output file path")
	flag.Parse()

	path := *cfgPath
	if flag.NArg() > 0 && path == "sqlgen.yaml" {
		path = flag.Arg(0)
	}

	if *outPath == "" {
		if err := sqlgen.GenerateFile(path); err != nil {
			fmt.Fprintf(os.Stderr, "sqlgen: %v\n", err)
			os.Exit(1)
		}
		return
	}

	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sqlgen: read %s: %v\n", path, err)
		os.Exit(1)
	}
	cfg, err := sqlgen.ParseYAML(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sqlgen: %v\n", err)
		os.Exit(1)
	}
	cfg.Out = *outPath
	src, err := sqlgen.Generate(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sqlgen: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*outPath, src, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "sqlgen: write %s: %v\n", *outPath, err)
		os.Exit(1)
	}
}
