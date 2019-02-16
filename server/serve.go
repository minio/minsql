/*
 * MinSQL, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package server

import (
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/minio/cli"
)

var minSQLDefaultPort = "9999"

// global flags for minsql.
var globalFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":" + minSQLDefaultPort,
		Usage: "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname",
	},
	cli.StringFlag{
		Name:  "minio-endpoint",
		Usage: "minio endpoint of form http(s)://<hostname>:<port> to persist incoming data",
	},
	cli.StringFlag{
		Name:  "minio-accesskey",
		Usage: "minio endpoint access key",
	},
	cli.StringFlag{
		Name:  "minio-secretkey",
		Usage: "minio endpoint secret key",
	},
}

// Help template for minsql.
var minsqlHelpTemplate = `NAME:
  {{.Name}}

DESCRIPTION:
  {{.Description}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}COMMAND{{if .VisibleFlags}}{{end}} [ARGS...]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
VERSION:
  ` + Version +
	`{{ "\n"}}`

func newApp(name string) *cli.App {
	// Set up app.
	cli.HelpFlag = cli.BoolFlag{
		Name:  "help, h",
		Usage: "Show help.",
	}

	app := cli.NewApp()
	app.Name = name
	app.Author = "MinIO, Inc."
	app.Version = Version
	app.Description = `Distributed SQL based search engine for log data`
	app.Flags = globalFlags
	app.HideHelpCommand = true // Hide `help, h` command, we already have `minsql --help`.
	app.CustomAppHelpTemplate = minsqlHelpTemplate

	httpServerErrorCh := make(chan error)
	osSignalCh := make(chan os.Signal, 1)
	app.Action = func(ctx *cli.Context) {
		address := ctx.GlobalString("address")

		server, tlsCerts, err := newHTTPServer(address)
		if err != nil {
			log.Fatalln(err)
		}

		server.Handler, err = configureMinSQLHandler(ctx)
		if err != nil {
			log.Fatalln(err)
		}

		go func() {
			httpServerErrorCh <- server.ListenAndServe()
		}()

		signal.Notify(osSignalCh, os.Interrupt, syscall.SIGTERM)

		log.Printf("MinSQL now listening on %s\n", address)

		handleSignals(server, tlsCerts, httpServerErrorCh, osSignalCh)

	}
	return app
}

// Serve serves minsql server.
func Serve(args []string) {
	// Set the minsql app name.
	appName := filepath.Base(args[0])

	// Run the app - exit on error.
	if err := newApp(appName).Run(args); err != nil {
		os.Exit(1)
	}
}
