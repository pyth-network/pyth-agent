import typing
from anchorpy.error import ProgramError


class CallerNotAllowed(ProgramError):
    def __init__(self) -> None:
        super().__init__(6000, "CPI Caller not allowed")

    code = 6000
    name = "CallerNotAllowed"
    msg = "CPI Caller not allowed"


class DuplicateAllowedProgram(ProgramError):
    def __init__(self) -> None:
        super().__init__(6001, "Whitelist already contains program")

    code = 6001
    name = "DuplicateAllowedProgram"
    msg = "Whitelist already contains program"


class ConversionError(ProgramError):
    def __init__(self) -> None:
        super().__init__(6002, "Conversion Error")

    code = 6002
    name = "ConversionError"
    msg = "Conversion Error"


class SerializeError(ProgramError):
    def __init__(self) -> None:
        super().__init__(6003, "Serialization Error")

    code = 6003
    name = "SerializeError"
    msg = "Serialization Error"


class WhitelistAdminRequired(ProgramError):
    def __init__(self) -> None:
        super().__init__(6004, "Whitelist admin required on initialization")

    code = 6004
    name = "WhitelistAdminRequired"
    msg = "Whitelist admin required on initialization"


class InvalidAllowedProgram(ProgramError):
    def __init__(self) -> None:
        super().__init__(6005, "Invalid allowed program")

    code = 6005
    name = "InvalidAllowedProgram"
    msg = "Invalid allowed program"


class MaximumAllowedProgramsExceeded(ProgramError):
    def __init__(self) -> None:
        super().__init__(6006, "Maximum number of allowed programs exceeded")

    code = 6006
    name = "MaximumAllowedProgramsExceeded"
    msg = "Maximum number of allowed programs exceeded"


class InvalidPDA(ProgramError):
    def __init__(self) -> None:
        super().__init__(6007, "Invalid PDA")

    code = 6007
    name = "InvalidPDA"
    msg = "Invalid PDA"


class CurrentDataLengthExceeded(ProgramError):
    def __init__(self) -> None:
        super().__init__(6008, "Update data exceeds current length")

    code = 6008
    name = "CurrentDataLengthExceeded"
    msg = "Update data exceeds current length"


class MessageBufferNotProvided(ProgramError):
    def __init__(self) -> None:
        super().__init__(6009, "Message Buffer not provided")

    code = 6009
    name = "MessageBufferNotProvided"
    msg = "Message Buffer not provided"


class FundBumpNotFound(ProgramError):
    def __init__(self) -> None:
        super().__init__(6010, "Fund Bump not found")

    code = 6010
    name = "FundBumpNotFound"
    msg = "Fund Bump not found"


CustomError = typing.Union[
    CallerNotAllowed,
    DuplicateAllowedProgram,
    ConversionError,
    SerializeError,
    WhitelistAdminRequired,
    InvalidAllowedProgram,
    MaximumAllowedProgramsExceeded,
    InvalidPDA,
    CurrentDataLengthExceeded,
    MessageBufferNotProvided,
    FundBumpNotFound,
]
CUSTOM_ERROR_MAP: dict[int, CustomError] = {
    6000: CallerNotAllowed(),
    6001: DuplicateAllowedProgram(),
    6002: ConversionError(),
    6003: SerializeError(),
    6004: WhitelistAdminRequired(),
    6005: InvalidAllowedProgram(),
    6006: MaximumAllowedProgramsExceeded(),
    6007: InvalidPDA(),
    6008: CurrentDataLengthExceeded(),
    6009: MessageBufferNotProvided(),
    6010: FundBumpNotFound(),
}


def from_code(code: int) -> typing.Optional[CustomError]:
    maybe_err = CUSTOM_ERROR_MAP.get(code)
    if maybe_err is None:
        return None
    return maybe_err
