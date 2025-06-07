
# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

 
import comm
import pytest

from ipykernel.comm import Comm


def test_create_comm():
    assert isinstance(comm.create_comm(), Comm)
