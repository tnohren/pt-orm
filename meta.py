from context import Context

class Meta(type):
    """
    Meta for ORM Models.
    """

    _context: Context = Context

    def _get_context(self) -> Context:
        return self._context(self)
    
    @property
    def context(self) -> Context:
        return self._get_context()
