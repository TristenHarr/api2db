# -*- coding: utf-8 -*-
"""
Contains the Post class
=======================
"""
from ..base_lam import BaseLam
from typing import Any


class Post(BaseLam):
    """Used as a BaseClass for all PostProcessors"""

    @staticmethod
    def typecast(dtype: Any) -> str:
        """
        Yields a string that can be used for typecasting to pandas dtype.

        Args:
            dtype: A python native type

        Returns:
            A string that can be used in conjunction with a pandas DataFrame/Series for typecasting
        """
        if dtype is int:
            return "Int64"
        elif dtype is float:
            return "Float64"
        elif dtype is bool:
            return "bool"
        return "string"
