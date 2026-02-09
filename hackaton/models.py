import enum

from sqlalchemy import String, func
from sqlalchemy.orm import Mapped, mapped_column, registry

from datetime import datetime

from sqlalchemy import Enum

table_registry = registry()

class UserPermission(enum.Enum):
    ADMIN = 'admin'
    COMUNITY = 'comunity'

class SituationType(enum.Enum):
    IMPROVING = 'improving'
    STABLE = 'stable'
    WORSING = 'worsing'


class ParseUserPermission:
    def __init__(self, string):
        self.string = string
    
    def getUserPermission(self):
        return UserPermission.ADMIN if self.string == 'admin' else UserPermission.COMUNITY

class ParseSituationType:
      def __init__(self, string):
        self.string = string
    
      def getSituationType(self):
        if self.string == 'improving':
            return SituationType.IMPROVING
        elif self.string == 'stable':
            return SituationType.STABLE
        elif self.string == 'worsing':
            return SituationType.WORSING
        else:
            raise TypeError('Invalid situation!')




@table_registry.mapped_as_dataclass
class UserModel:
    __tablename__='users'

    id: Mapped[int] = mapped_column(init=False, primary_key=True)

    username: Mapped[str] = mapped_column(String(30), unique=True)
    
    email: Mapped[str] = mapped_column(String(30), unique=True)

    password:Mapped[str]

    role: Mapped[UserPermission] = mapped_column(Enum(UserPermission), default=UserPermission.COMUNITY)
 
    created_at:Mapped[datetime] = mapped_column(init=False, server_default=func.now())


@table_registry.mapped_as_dataclass
class AuditResultModel:

    __tablename__='audit_results'

    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    
    date:Mapped[datetime]
    
    line:Mapped[int]

    clear_pm: Mapped[str]

    ref_qtd_sum: Mapped[int]

    ref_freq_sum:Mapped[int]

    ref_formal_sum:Mapped[int]

    ref_informal_sum:Mapped[int]

    nc_total_sum:Mapped[int]

    opened_nc_sum:Mapped[int]

    priority:Mapped[int]

    status:Mapped[bool] 
    
    description:Mapped[str]

    situation:Mapped[SituationType] = mapped_column(Enum(SituationType), default=SituationType.STABLE)


    created_at:Mapped[datetime] = mapped_column(init=False, server_default=func.now())