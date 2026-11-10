"""HTTP transport for credentialed operations against explicitly configured peers."""
import urllib.request


class RefuseRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # urllib forwards Authorization on redirects, including another origin.
        # Refuse same-origin redirects too: a write must not become a GET.
        return None


_OPENER = urllib.request.build_opener(RefuseRedirects())


def open_without_redirects(request, *, timeout):
    # Keep urllib's normal proxy handling, CA trust and HTTPS verification.
    return _OPENER.open(request, timeout=timeout)
