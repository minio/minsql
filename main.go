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

/*
 * Below main package has canonical imports for 'go get' and 'go build'
 * to work with all other clones of github.com/minio/minio repository. For
 * more information refer https://golang.org/doc/go1.4#canonicalimports
 */

package main // import "github.com/minio/minsql"

import (
	"fmt"
	"os"
	"runtime"

	version "github.com/hashicorp/go-version"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/minsql/server"
)

const (
	// Minio requires at least Go v1.12
	minGoVersion        = "1.12"
	goVersionConstraint = ">= " + minGoVersion
)

// Check if this binary is compiled with at least minimum Go version.
func checkGoVersion(goVersionStr string) error {
	constraint, err := version.NewConstraint(goVersionConstraint)
	if err != nil {
		return fmt.Errorf("'%s': %s", goVersionConstraint, err)
	}

	goVersion, err := version.NewVersion(goVersionStr)
	if err != nil {
		return err
	}

	if !constraint.Check(goVersion) {
		return fmt.Errorf("Minio is not compiled by go %s. Minimum required version is %s, go %s release is known to have security issues. Please recompile accordingly", goVersionConstraint, minGoVersion, runtime.Version()[2:])
	}

	return nil
}

func main() {
	// When `go get` is used minimum Go version check is not triggered but it would have compiled it successfully.
	// However such binary will fail at runtime, hence we also check Go version at runtime.
	if err := checkGoVersion(runtime.Version()[2:]); err != nil {
		console.Errorln(err)
	}

	server.Serve(os.Args)
}
