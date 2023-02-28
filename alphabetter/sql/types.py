import sqlalchemy.types
import uuid
import alphabetter.core.model


class UUID(sqlalchemy.types.UserDefinedType):
    cache_ok = True

    def get_col_spec(self, **kwargs):
        return 'VARCHAR(36)'

    def bind_processor(self, dialect):
        def process(value: uuid.UUID):
            return str(value)
        return process

    def result_processor(self, dialect, coltype):
        def process(value: str):
            return uuid.UUID(value)
        return process


class Country(sqlalchemy.types.UserDefinedType):
    cache_ok = True

    def get_col_spec(self, **kwargs):
        return 'VARCHAR(2)'

    def bind_processor(self, dialect):
        def process(value: Country):
            return str(value)
        return process

    def result_processor(self, dialect, coltype):
        def process(value: str):
            return alphabetter.core.model.Country(value) # type: ignore
        return process
