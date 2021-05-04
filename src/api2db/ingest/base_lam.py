# -*- coding: utf-8 -*-
"""
Contains the BaseLam class
==========================
"""
import dill
from typing import Any


class BaseLam(object):
    """Used as a Base object for pre-process subclasses, post-process subclasses, and data-features."""

    def __call__(self, lam_arg: Any) -> Any:
        """
        Makes the class callable, with target of class method `lam_wrap`
        This is used to allow for anonymous functions to be passed to the class, and to enhance
        ease of use for library developers.

        Args:
            lam_arg: The argument to be passed to the `lam_wrap` class method.

        Returns:
            The response of the `lam_wrap` class method
        """
        return self.lam_wrap(lam_arg)

    def __getstate__(self) -> dict:
        """
        Allows for lambda operations to be serialized in order to allow for instance to be passed between processes

        Returns:
            Customized self.__dict__ items with values serialized using the `dill` library
        """
        return {k: dill.dumps(v) for k, v in self.__dict__.items()}

    def __setstate__(self, state: dict) -> None:
        """
        Allows for lambda operations to be deserialized using the `dill` library in order to allow for instance to
        be passed between processes

        Args:
            state: Incoming state

        Returns:
            None
        """
        self.__dict__ = {k: dill.loads(v) for k, v in state.items()}

    def lam_wrap(self, lam_arg: Any) -> None:
        """
        Method that performs class lambda method on ``lam_arg``
        This method will **ALWAYS** be overriden.

        Args:
            lam_arg: The incoming data to perform the lambda operation on.

        Returns:
            None if attempting to call ``BaseLam.lam_wrap``, return is dictated by subclasses.
        """
        pass
