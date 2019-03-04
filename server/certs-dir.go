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
	"errors"
	"os"
	"path/filepath"

	"github.com/minio/cli"
	homedir "github.com/mitchellh/go-homedir"
)

const (
	// Default minsql certs directory where below certs files/directories are stored.
	defaultMinSQLCertsDir = ".minsql"

	// Directory contains all CA certificates other than system defaults for HTTPS.
	certsCADir = "CAs"

	// Public certificate file for HTTPS.
	publicCertFile = "public.crt"

	// Private key file for HTTPS.
	privateKeyFile = "private.key"
)

// CertsDir - points to a user set directory.
type CertsDir struct {
	path string
}

func getDefaultCertsDir() string {
	homeDir, err := homedir.Dir()
	if err != nil {
		return ""
	}

	return filepath.Join(homeDir, defaultMinSQLCertsDir)
}

func getDefaultCertsCADir() string {
	return filepath.Join(getDefaultCertsDir(), certsCADir)
}

var (
	// Default config, certs and CA directories.
	defaultCertsDir   = &CertsDir{path: getDefaultCertsDir()}
	defaultCertsCADir = &CertsDir{path: getDefaultCertsCADir()}

	// Points to current certs directory set by user with --certs-dir
	globalCertsDir = defaultCertsDir
	// Points to relative path to certs directory and is <value-of-certs-dir>/CAs
	globalCertsCADir = defaultCertsCADir
)

// Get - returns current directory.
func (dir *CertsDir) Get() string {
	return dir.path
}

// Attempts to create all directories, ignores any permission denied errors.
func mkdirAllIgnorePerm(path string) error {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		// It is possible in kubernetes like deployments this directory
		// is already mounted and is not writable, ignore any write errors.
		if os.IsPermission(err) {
			err = nil
		}
	}
	return err
}

func getPublicCertFile() string {
	return filepath.Join(globalCertsDir.Get(), publicCertFile)
}

func getPrivateKeyFile() string {
	return filepath.Join(globalCertsDir.Get(), privateKeyFile)
}

func newCertsDirFromCtx(ctx *cli.Context, option string, getDefaultDir func() string) (*CertsDir, error) {
	var dir string

	switch {
	case ctx.IsSet(option):
		dir = ctx.String(option)
	case ctx.GlobalIsSet(option):
		dir = ctx.GlobalString(option)
		// cli package does not expose parent's option option.  Below code is workaround.
		if dir == "" || dir == getDefaultDir() {
			if ctx.Parent().GlobalIsSet(option) {
				dir = ctx.Parent().GlobalString(option)
			}
		}
	default:
		// Neither local nor global option is provided.  In this case, try to use
		// default directory.
		dir = getDefaultDir()
	}

	if dir == "" {
		return nil, errors.New("empty directory")
	}

	// Disallow relative paths, figure out absolute paths.
	dirAbs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	if err = mkdirAllIgnorePerm(dirAbs); err != nil {
		return nil, err
	}

	return &CertsDir{path: dirAbs}, nil
}
