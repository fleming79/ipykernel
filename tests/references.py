# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

# References are used for data validation

from __future__ import annotations

import re
import sys

from packaging.version import Version as PackingVersion
from traitlets import Bool, Dict, Enum, HasTraits, Integer, List, TraitError, Unicode, observe

__all__ = ["Reference", "references"]


class Reference(HasTraits):
    """
    Base class for message spec specification testing.

    This class is the core of the message specification test.  The
    idea is that child classes implement trait attributes for each
    message keys, so that message keys can be tested against these
    traits using :meth:`check` method.

    """

    def __str__(self):
        return str(self.__class__)

    def check(self, d):
        """validate a dict against our traits"""
        for key in self.trait_names():
            if key not in d:
                msg = f"{key=} is missing for {self} in {d=}"
                raise KeyError(msg)
            # FIXME: always allow None, probably not a good idea
            if d[key] is None:
                continue
            try:
                setattr(self, key, d[key])
            except TraitError as e:
                e.add_note(f"Validation failed for {key=} with  value:{d[key]}")
                raise


class Version(Unicode):
    def __init__(self, *args, **kwargs):
        self.min = kwargs.pop("min", None)
        self.max = kwargs.pop("max", None)
        kwargs["default_value"] = self.min
        super().__init__(*args, **kwargs)

    def validate(self, obj, value):
        if self.min and PackingVersion(value) < PackingVersion(self.min):
            msg = f"bad version: {value} < {self.min}"
            raise TraitError(msg)
        if self.max and (PackingVersion(value) > PackingVersion(self.max)):
            msg = f"bad version: {value} > {self.max}"
            raise TraitError(msg)


class RMessage(Reference):
    msg_id = Unicode()
    msg_type = Unicode()
    header = Dict()
    parent_header = Dict()
    content = Dict()

    def check(self, d):
        super().check(d)
        RHeader().check(self.header)
        if self.parent_header:
            RHeader().check(self.parent_header)


class RHeader(Reference):
    msg_id = Unicode()
    msg_type = Unicode()
    session = Unicode()
    username = Unicode()
    version = Version(min="5.0")


mime_pat = re.compile(r"^[\w\-\+\.]+/[\w\-\+\.]+$")


class MimeBundle(Reference):
    metadata = Dict()
    data = Dict()

    @observe("data")
    def _on_data_changed(self, change):
        for k, v in change["new"].items():
            assert mime_pat.match(k)
            assert isinstance(v, str)


# shell replies
class Reply(Reference):
    status = Enum(("ok", "error"), default_value="ok")


class ExecuteReply(Reply):
    execution_count = Integer()

    def check(self, d):
        super().check(d)
        if d["status"] == "ok":
            ExecuteReplyOkay().check(d)
        elif d["status"] == "error":
            ExecuteReplyError().check(d)
        elif d["status"] == "aborted":
            "Deprectated"
            raise NotImplementedError
            ExecuteReplyAborted().check(d)


class ExecuteReplyOkay(Reply):
    status = Enum("ok")
    user_expressions = Dict()


class ExecuteReplyError(Reply):
    status = Enum("error")
    ename = Unicode()
    evalue = Unicode()
    traceback = List(Unicode())


class ExecuteReplyAborted(Reply):
    status = Enum("aborted")


class InspectReply(Reply, MimeBundle):
    found = Bool()


class ArgSpec(Reference):
    args = List(Unicode())
    varargs = Unicode()
    varkw = Unicode()
    defaults = List()


class Status(Reference):
    execution_state = Enum(("busy", "idle", "starting"), default_value="busy")


class CompleteReply(Reply):
    matches = List(Unicode())
    cursor_start = Integer()
    cursor_end = Integer()
    status = Unicode()  # type:ignore


class LanguageInfo(Reference):
    name = Unicode("python")
    version = Unicode(sys.version.split()[0])


class KernelInfoReply(Reply):
    protocol_version = Version(min="5.0")
    implementation = Unicode("ipython")
    implementation_version = Version(min="2.1")
    language_info = Dict()
    banner = Unicode()

    def check(self, d):
        super().check(d)
        LanguageInfo().check(d["language_info"])


class ConnectReply(Reference):
    shell_port = Integer()
    control_port = Integer()
    stdin_port = Integer()
    iopub_port = Integer()
    hb_port = Integer()


class CommInfoReply(Reply):
    comms = Dict()


class IsCompleteReply(Reference):
    status = Enum(("complete", "incomplete", "invalid", "unknown"), default_value="complete")

    def check(self, d):
        super().check(d)
        if d["status"] == "incomplete":
            IsCompleteReplyIncomplete().check(d)


class IsCompleteReplyIncomplete(Reference):
    indent = Unicode()


# IOPub messages


class ExecuteInput(Reference):
    code = Unicode()
    execution_count = Integer()


class Error(ExecuteReplyError):
    """Errors are the same as ExecuteReply, but without status"""

    status = None  # type: ignore[assignment]  # no status field


class Stream(Reference):
    name = Enum(("stdout", "stderr"), default_value="stdout")
    text = Unicode()


class DisplayData(MimeBundle):
    pass


class ExecuteResult(MimeBundle):
    execution_count = Integer()


class HistoryReply(Reply):
    history = List(List())


# Subshell control messages


class CreateSubshellReply(Reply):
    subshell_id = Unicode()


class DeleteSubshellReply(Reply):
    pass


class ListSubshellReply(Reply):
    subshell_id = List(Unicode())


references = {
    "execute_reply": ExecuteReply(),
    "inspect_reply": InspectReply(),
    "status": Status(),
    "complete_reply": CompleteReply(),
    "kernel_info_reply": KernelInfoReply(),
    "connect_reply": ConnectReply(),
    "comm_info_reply": CommInfoReply(),
    "is_complete_reply": IsCompleteReply(),
    "execute_input": ExecuteInput(),
    "execute_result": ExecuteResult(),
    "history_reply": HistoryReply(),
    "error": Error(),
    "stream": Stream(),
    "display_data": DisplayData(),
    "header": RHeader(),
    "create_subshell_reply": CreateSubshellReply(),
    "delete_subshell_reply": DeleteSubshellReply(),
    "list_subshell_reply": ListSubshellReply(),
}
