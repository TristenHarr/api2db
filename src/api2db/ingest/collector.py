# -*- coding: utf-8 -*-
"""
Contains the Collector class
============================
"""
from .api_form import ApiForm
from ..stream.stream import Stream
from ..store.store import Store
from typing import Callable, List, Union
from multiprocessing import Queue


class Collector(object):
    """Used for creating a data-collection pipeline from API to storage medium"""

    def __init__(self,
                 name: str,
                 seconds: int,
                 import_target: Callable[[], Union[List[dict], None]],
                 api_form: Callable[[], ApiForm],
                 streams: Callable[[], List[Stream]],
                 stores: Callable[[], List[Store]],
                 debug: bool = True):
        """
        Creates a Collector object

        NOTE:

            Project collectors are disabled by default, this allows the project to run immediately after ``pmake`` is
            run without any code being written. To enable a collector, you must change its ``seconds`` parameter to a
            number greater than zero. This represents the periodic interval that the collectors ``import_target`` is
            run. I.e. The collector will request data from its configured API every ``seconds`` seconds.

            A perceptive user may notice that ``import_target``, ``api_form``, ``streams`` and ``stores`` appear to
            be written in seemingly extraneous functions. Why not just pass in the actual data directly to the
            Collector object? This occurs due to the extensive use of anonymous functions which is what allows the
            library to be so expressive. Python's native serialization does not support serializing lambdas. When
            using the multiprocessing module and spawning a new process the parameters of the process are serialized
            before being piped into a new python interpreter instance. It is for this reason that functions are used
            as parameters rather than their returns, since it is possible to pass a function which *will* instantiate
            an anonymous function upon call, but not to pass an existing anonymous function to a separate process.
            Feel free to write a supporting package to make it so this is not the case.

        Args:
            name: The name of the collector, this name will be set when using ``pmake`` or ``cadd`` and **should not be
                  changed**. Changing this may result in unintended functionality of the api2db library, as this name is
                  used when determining where to store incoming data, what to name database tables, and the location of
                  the dtypes file which gets stored in the projects `CACHE/` directory. If you wish to change the name
                  of a collector, you can run ``cadd`` to add a new collector with the desired name, and then move the
                  code from the old collector into the new collector.
            seconds: This specifies the periodic interval that data should be imported at.
                     I.e. ``seconds=30`` will request data from the collector api every 30 seconds. This is set to
                     `0` by default, and when set to `0` the collector is disabled and will not be registered with
                     the main program. This allows for all neccesary collectors to be added to a project, and then for
                     each collector to be enabled as its code is written.
            import_target: The import_target is the function that the programmer using the library writes that performs
                           the initial data import. In most cases this will utilize a library like `urllib` in order
                           to perform the requests. The return of this function should be a list of dictionary objects.

                           * When dealing with XML data use a library like `xmltodict` to convert the data to a python
                             dictionary

                           * When dealing with JSON data use a library like the built-in `json` library to convert the
                             data to a python dictionary.

                           The implementation of this method is left to the programmer. This method could also be
                           written to collect data from a serial stream, or a web-scraper if desired. Design and
                           implementation of things such as that are left to the users of the library.

                           The ``import_target`` **MUST return a list of dictionaries, or None**. Exceptions that may
                           occur within the function must be handled.
                           The purpose of this implementation is to allow for logic to be written to perform multiple
                           API requests and treat the data as a single incoming request. Most APIs will return a single
                           response, and if the implementation of the ``import_target`` does not make multiple API calls
                           then simply wrap that data in a list when returning it from the function.
            api_form: This is a function that returns an API form.
            streams: This is a function that returns a list of Stream object subclasses.
            stores: This is a function that returns a list of Store object subclasses.
            debug: When set to True logs will be printed to the console. Set to False for production.
        """
        self.name = name
        self.seconds = seconds
        self.import_target = import_target
        self.api_form = api_form
        self.streams = streams
        self.stores = stores
        self.debug = debug
        self.q = None
        """Optional[multiprocessing.Queue]: A queue used for message passing if collector is running in debug mode"""

    def set_q(self, q: Queue) -> None:
        """
        Sets the q class member used for collectors running in debug mode

        Args:
            q: The queue used for message passing

        Returns:
            None
        """
        self.q = q
