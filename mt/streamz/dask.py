'''Extra dask operators for streamz.'''


from tornado import gen
from dask.distributed import default_client
from streamz import DaskStream, logger
from . import core


__all__ = ['DaskStream', 'gather_ignore']


@DaskStream.register_api()
class rebatch(DaskStream, core.rebatch):
    pass


@DaskStream.register_api()
class partition2(DaskStream, core.partition2):
    pass


@DaskStream.register_api()
class batch_map(DaskStream):
    __doc__ = core.batch_map.__doc__

    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.args = args

        DaskStream.__init__(self, upstream)

    def update(self, batch, who=None, metadata=None):
        try:
            client = default_client()
            result = [client.submit(self.func, x, *self.args, **self.kwargs) for x in batch]
        except Exception as e:
            logger.exception(e)
            raise
        else:
            return self._emit(result, metadata=metadata)


@DaskStream.register_api()
class gather_ignore(DaskStream):
    """ Wait on and gather results from DaskStream to local Stream

    This operates the same as the 'gather' operator. However, if an exception is raised from the client, we ignore the Future object from the client and replace to upstream with a default object.

    Parameters
    ----------
    obj : object
        default object to return to if an exception from the client is caught
    logger : IndentedLoggerAdapter, optional
        logger to debug if an exception is raised

    Examples
    --------
    >>> local_stream = dask_stream.buffer(20).gather_ignore(None)

    See Also
    --------
    buffer
    scatter

    Notes
    -----
    This op is no op within Stream. It only is effective within DaskStream.
    """

    def __init__(self, upstream, default_obj, logger=None, *args, **kwargs):
        self.default_obj = default_obj
        self.logger = logger
        DaskStream.__init__(self, upstream, *args, **kwargs)

    @gen.coroutine
    def update(self, x, who=None, metadata=None):
        client = default_client()

        self._retain_refs(metadata)

        try:
            result = yield client.gather(x, asynchronous=True)
            result2 = yield self._emit(result, metadata=metadata)
        except:
            if self.logger:
                self.logger.warn_last_exception()
                self.logger.warn("Ignored the above exception, returning the default object.")
            result2 = yield self._emit(self.default_obj, metadata=metadata)

        self._release_refs(metadata)

        raise gen.Return(result2)


