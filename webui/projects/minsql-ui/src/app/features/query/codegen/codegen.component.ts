// This file is part of MinSQL
// Copyright (c) 2019 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

import {AfterViewInit, ChangeDetectionStrategy, Component, Input, OnInit, QueryList, ViewChildren} from '@angular/core';
import {ClipboardService} from 'ngx-clipboard';
import {NotificationService} from '../../../core/core.module';
import {isNullOrUndefined} from 'util';

declare var PR;


@Component({
  selector: 'minsql-codegen',
  templateUrl: './codegen.component.html',
  styleUrls: ['./codegen.component.scss'],
  changeDetection: ChangeDetectionStrategy.Default
})
export class CodegenComponent implements OnInit, AfterViewInit {

  _query: string;

  selectedLang = null;

  codeTemplates = [
    {
      title: 'CURL',
      lang: 'lang-bash',
      template: 'curl -X POST \\\n' +
        ' http://minsql:9999/search \\\n' +
        ' -H \'MINSQL-TOKEN: <YOUR_TOKEN>\' \\\n' +
        ' -d \'<!query>\''
    },
    {
      title: 'Python',
      lang: 'lang-python',
      template: 'import requests\n' +
        ' \n' +
        'url = \'http://minsq:9999/search\'\n' +
        'data = \'<!query>\'\n' +
        'headers = {\'MINSQL-TOKEN\': \'<YOUR_TOKEN>\'}\n' +
        'r = requests.post(url, data=data, headers=headers)\n' +
        'print(r.content)'
    },
    {
      title: 'Go',
      lang: 'lang-go',
      template: 'package main\n' +
        '\n' +
        'import (\n' +
        '    "bytes"\n' +
        '    "fmt"\n' +
        '    "io/ioutil"\n' +
        '    "net/http"\n' +
        ')\n' +
        '\n' +
        'func main() {\n' +
        '    url := "http://minsql:9999/search"\n' +
        '    var post_body = []byte(`<!query>`)\n' +
        '    req, err := http.NewRequest("POST", url, bytes.NewBuffer(post_body))\n' +
        '    req.Header.Set("MINSQL-TOKEN", "<YOUR_TOKEN>")\n' +
        '\n' +
        '    client := &http.Client{}\n' +
        '    resp, err := client.Do(req)\n' +
        '    if err != nil {\n' +
        '        panic(err)\n' +
        '    }\n' +
        '    defer resp.Body.Close()\n' +
        '\n' +
        '    fmt.Println("response Status:", resp.Status)\n' +
        '    fmt.Println("response Headers:", resp.Header)\n' +
        '    body, _ := ioutil.ReadAll(resp.Body)\n' +
        '    fmt.Println("response Body:", string(body))\n' +
        '}\n'
    },
    {
      title: 'Java',
      lang: 'lang-java',
      template: 'import java.io.BufferedReader;\n' +
        'import java.io.DataOutputStream;\n' +
        'import java.io.IOException;\n' +
        'import java.io.InputStreamReader;\n' +
        'import java.net.HttpURLConnection;\n' +
        'import java.net.MalformedURLException;\n' +
        'import java.net.ProtocolException;\n' +
        'import java.net.URL;\n' +
        'import java.nio.charset.StandardCharsets;\n' +
        '\n' +
        'public class post {\n' +
        '\n' +
        '    private static HttpURLConnection con;\n' +
        '\n' +
        '    public static void main(String[] args) throws MalformedURLException,\n' +
        '\t\tProtocolException, IOException {\n' +
        '\n' +
        '\tString url = "http://minsql:9999/search";\n' +
        '\tString urlParameters = "<!query>";\n' +
        '\tbyte[] postData = urlParameters.getBytes(StandardCharsets.UTF_8);\n' +
        '\n' +
        '\ttry {\n' +
        '\n' +
        '\t\tURL myurl = new URL(url);\n' +
        '\t\tcon = (HttpURLConnection) myurl.openConnection();\n' +
        '\n' +
        '\t\tcon.setDoOutput(true);\n' +
        '\t\tcon.setRequestMethod("POST");\n' +
        '\t\tcon.setRequestProperty("MINSQL-TOKEN", "<YOUR_TOKEN>");\n' +
        '\n' +
        '\t\ttry (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {\n' +
        '\t\t\twr.write(postData);\n' +
        '\t\t}\n' +
        '\n' +
        '\t\tStringBuilder content;\n' +
        '\n' +
        '\t\ttry (BufferedReader in = new BufferedReader(\n' +
        '\t\t\t    new InputStreamReader(con.getInputStream()))) {\n' +
        '\n' +
        '\t\t\tString line;\n' +
        '\t\t\tcontent = new StringBuilder();\n' +
        '\n' +
        '\t\t\twhile ((line = in.readLine()) != null) {\n' +
        '\t\t\t    content.append(line);\n' +
        '\t\t\t    content.append(System.lineSeparator());\n' +
        '\t\t\t}\n' +
        '\t\t}\n' +
        '\n' +
        '\t\tSystem.out.println(content.toString());\n' +
        '\n' +
        '\t} finally {\n' +
        '\t\t\n' +
        '\t\tcon.disconnect();\n' +
        '\t}\n' +
        '    }\n' +
        '}'
    },
    {
      title: 'Ruby',
      lang: 'lang-ruby',
      template: 'require \'net/http\'\n' +
        'require \'uri\'\n' +
        'require \'json\'\n' +
        '\n' +
        'uri = URI.parse("http://minsql:9999/search")\n' +
        '\n' +
        'header = {\'MINSQL-TOKEN\': \'<YOUR_TOKEN>\'}\n' +
        'query = \'<!query>\'\n' +
        '\n' +
        '# Create the HTTP objects\n' +
        'http = Net::HTTP.new(uri.host, uri.port)\n' +
        'request = Net::HTTP::Post.new(uri.request_uri, header)\n' +
        'request.body = query\n' +
        '\n' +
        '# Send the request\n' +
        'response = http.request(request)\n' +
        '\n' +
        'print(response.body)'
    },
    {
      title: 'NodeJS',
      lang: 'lang-node',
      template: '// We need this to build our post string\n' +
        'var querystring = require(\'querystring\');\n' +
        'var http = require(\'http\');\n' +
        'var fs = require(\'fs\');\n' +
        '\n' +
        '// Build the post string from an object\n' +
        'var post_data = \'<!query>\';\n' +
        '\n' +
        '// An object of options to indicate where to post to\n' +
        'var post_options = {\n' +
        '    host: \'minsql\',\n' +
        '    port: \'9999\',\n' +
        '    path: \'/search\',\n' +
        '    method: \'POST\',\n' +
        '    headers: {\n' +
        '\t\'Content-Type\': \'text/plain\',\n' +
        '\t\'Content-Length\': Buffer.byteLength(post_data),\n' +
        '\t\'MINSQL-TOKEN\': \'<YOUR_TOKEN>\'\n' +
        '    }\n' +
        '};\n' +
        '\n' +
        '// Set up the request\n' +
        'var post_req = http.request(post_options, function(res) {\n' +
        '    res.setEncoding(\'utf8\');\n' +
        '    res.on(\'data\', function (chunk) {\n' +
        '\tconsole.log(\'Response: \' + chunk);\n' +
        '    });\n' +
        '});\n' +
        '\n' +
        '// post the data\n' +
        'post_req.write(post_data);\n' +
        'post_req.end();\n' +
        '\n'
    },
    {
      title: 'PHP',
      lang: 'lang-php',
      template: '<?php\n' +
        '//The url you wish to send the POST request to\n' +
        '$url = \'http://minsql:9999/search\';\n' +
        '\n' +
        '//The data you want to send via POST\n' +
        '$data = \'<!query>\';\n' +
        '\n' +
        '//open connection\n' +
        '$ch = curl_init();\n' +
        '\n' +
        '//set the url, number of POST vars, POST data\n' +
        'curl_setopt($ch,CURLOPT_URL, $url);\n' +
        'curl_setopt($ch,CURLOPT_POST, true);\n' +
        'curl_setopt($ch,CURLOPT_POSTFIELDS, $data);\n' +
        'curl_setopt($ch, CURLOPT_HTTPHEADER, array(\n' +
        '    \'MINSQL-TOKEN: abcdefghijklmnopabcdefghijklmnopabcdefghijklmnop\'\n' +
        '));\n' +
        '\n' +
        '//So that curl_exec returns the contents of the cURL; rather than echoing it\n' +
        'curl_setopt($ch,CURLOPT_RETURNTRANSFER, true); \n' +
        '\n' +
        '//execute post\n' +
        '$result = curl_exec($ch);\n' +
        'echo $result;\n' +
        '?>'
    },
    {
      title: '.NET',
      lang: 'lang-net',
      template: '// Post a query to MinSQL\n' +
        'using System;\n' +
        'using System.IO;\n' +
        'using System.Net;\n' +
        'using System.Net.Http;\n' +
        'using System.Text;\n' +
        '\n' +
        'namespace hello\n' +
        '{\n' +
        '\tclass MyClass\n' +
        '\t{\n' +
        '\t\tprivate static readonly HttpClient client = new HttpClient();\n' +
        '\n' +
        '\t\tstatic void Main()\n' +
        '\t\t{\n' +
        '\t\t\tvar request = (HttpWebRequest)WebRequest.Create("http://minsql:9999/search");\n' +
        '\n' +
        '\t\t\tvar postData = "<!query>";\n' +
        '\t\t\tvar data = Encoding.ASCII.GetBytes(postData);\n' +
        '\n' +
        '\t\t\trequest.Method = "POST";\n' +
        '\t\t\trequest.ContentType = "text/plain";\n' +
        '\t\t\trequest.ContentLength = data.Length;\n' +
        '\t\t\trequest.Headers["MINSQL-TOKEN"] = "<YOUR_TOKEN>";\n' +
        '\n' +
        '\t\t\tusing (var stream = request.GetRequestStream())\n' +
        '\t\t\t{\n' +
        '\t\t\t    stream.Write(data, 0, data.Length);\n' +
        '\t\t\t}\n' +
        '\n' +
        '\t\t\tvar response = (HttpWebResponse)request.GetResponse();\n' +
        '\n' +
        '\t\t\tvar responseString = new StreamReader(response.GetResponseStream()).ReadToEnd();\n' +
        '\t\t\tConsole.WriteLine(responseString);\n' +
        '\t\t}\n' +
        '\t}\n' +
        '}'
    }
  ];

  filteredValues = [];

  constructor(
    private clipboardService: ClipboardService,
    private notificationsService: NotificationService) {
  }

  @Input()
  set query(query) {
    this._query = query;
    this.selectChange(this.selectedLang);
    PR.prettyPrint();
  }

  get query() {
    return this._query;
  }

  @ViewChildren('allTheseThings') things: QueryList<any>;

  ngAfterViewInit(): void {
    this.things.changes.subscribe(t => {
      this.ngForRendred();
    });
    this.ngForRendred();
  }

  @Input()
  set ready(isReady: boolean) {
    this.ngForRendred();
  }

  ngForRendred() {
    PR.prettyPrint();
  }

  ngOnInit(): void {
    this.selectChange(null);
  }

  applyQuery() {
    for (const elem of this.filteredValues) {
      elem.display = elem.template.replace('<!query>', this.query);
    }
  }

  selectChange(event?) {
    if (!isNullOrUndefined(event)) {
      this.filteredValues = this.codeTemplates.filter(t => t.lang === event)
    } else {
      this.filteredValues = this.codeTemplates.filter(t => true);
    }
    this.applyQuery();
  }

  copyToPB(code) {
    this.clipboardService.copyFromContent(code.template);
    this.notificationsService.default('Copied to pasteboard');
  }

}
