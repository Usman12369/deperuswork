from http.server import BaseHTTPRequestHandler, HTTPServer
import threading


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return


def run_health_server(port=8080):
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"Health check server started on port {port}")
    server.serve_forever()


def start_health_server(port=8080):
    thread = threading.Thread(target=run_health_server, kwargs={"port": port}, daemon=True)
    thread.start()
    return thread
