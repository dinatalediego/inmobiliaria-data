class ScraperError(Exception):
    """Base error for the library."""


class FetchError(ScraperError):
    """Raised when a page cannot be fetched."""


class ParseError(ScraperError):
    """Raised when a page cannot be parsed."""
