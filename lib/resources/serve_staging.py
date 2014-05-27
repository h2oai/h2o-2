import SocketServer
import SimpleHTTPServer
import urllib, urlparse

import json
import os
import requests
import cgi, cgitb
import tempfile, StringIO

import mockApiV2 as mockAPIv2

import mockApiV2 as mockAPIv2

PORT = 8888

PROXY_ROUTE = '/passToH2O'
PROXY_PASS_TO_SERVER = "http://127.0.0.1:54321"

def isPassedPath(path):
    return path.startswith(PROXY_ROUTE)

def passedPathToRemoteUrl(path):
    passed_path = path[len(PROXY_ROUTE):]

    return anyPathToRemoteUrl(passed_path)

def anyPathToRemoteUrl(path):
    return "%s%s" % (PROXY_PASS_TO_SERVER, path)

def shouldBeMocked(path, method="GET"):
    return mockAPIv2.isMockAvailable(path, method)

def shouldMockUpload(path, method="GET"):
    return mockAPIv2.shouldMockUpload(path, method)

def mockResponse(path, params, method="GET"):
    return mockAPIv2.mockResponse(path, params, method)

class H2OJsonProxyRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler, object):
    def queryParams(self, method="GET"):
        if method == 'POST':
            length = int(self.headers['Content-Length'])
            request_data = self.rfile.read(length).decode('utf-8')

            query_urlparsed = urlparse.parse_qs(request_data)

            if query_urlparsed:
                return query_urlparsed
            else:
                return json.loads(request_data)

        else:
            return urlparse.parse_qs(urlparse.urlparse(self.path).query)

    def respondWithMockUpload(self, path, method="POST"):
        length = int(self.headers['Content-Length'])
        request_data = self.rfile.read(length)

        # cgitb.enable()

        fs = cgi.FieldStorage( fp = StringIO.StringIO(request_data),
                               headers = self.headers, # headers_,
                               environ={ 'REQUEST_METHOD': method } # all the rest will come from the 'headers' object,
                               # but as the FieldStorage object was designed for CGI, absense of 'POST' value in environ
                               # will prevent the object from using the 'fp' argument !
                             )

        # write to temp
        fs_up = fs['file']
        filename = os.path.split(fs_up.filename)[1]

        tempf = tempfile.TemporaryFile(mode="wb")
        tempf.write( fs_up.file.read() )
        tempf.close()

        status, response = mockResponse(path, self.queryParams("GET"), method)
        self.send_response(status)

        self.send_header('Content-type', 'application/json')
        self.end_headers()

        self.wfile.write(response)
        self.wfile.close()

    def respondWithMockResponse(self, path, method="GET"):
        # import time, random
        # time.sleep(1 + random.random() * 2)

        status, response = mockResponse(path, self.queryParams(method), method)
        self.send_response(status)

        self.send_header('Content-type', 'application/json')
        self.end_headers()

        self.wfile.write(response)
        self.wfile.close()

    def dealWithRequest(self, uri, method="GET"):
        path = uri.split("?")[0]
        if shouldBeMocked(path, method):
            if shouldMockUpload(path, method):
                self.respondWithMockUpload(path, method)
            else:
                self.respondWithMockResponse(path, method)
        else:
            self.process_passed_request(anyPathToRemoteUrl(uri))

    def do_POST(self):
        self.dealWithRequest(self.path, "POST")

    def do_GET(self):
        if isPassedPath(self.path):
            remote_url = passedPathToRemoteUrl(self.path)

            self.process_passed_request(remote_url)
        else:
            parsedParams = urlparse.urlparse(self.path)

            if os.access('.' + os.sep + parsedParams.path, os.R_OK) and self.path != "/":
                return super(H2OJsonProxyRequestHandler, self).do_GET()
            else:
                self.dealWithRequest(self.path, "GET")

    def process_passed_request(self, remote_url):
        r = requests.get(remote_url)

        self.send_response(r.status_code)
        for header, value in r.headers.iteritems():
            self.send_header(header, value)
        self.end_headers()

        self.wfile.write(r.text)
        self.wfile.close()

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

SocketServer.TCPServer.allow_reuse_address = True
httpd = ThreadedTCPServer(('', PORT), H2OJsonProxyRequestHandler)

print "serving at port", PORT

httpd.serve_forever()
