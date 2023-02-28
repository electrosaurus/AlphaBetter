import tqdm
import tqdm.notebook
import yaml

from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Type, Self, TextIO, ClassVar


@dataclass(kw_only=True)
class AppConfig:
    default_source: ClassVar[Path] = Path(__file__).parent.parent / 'configs' / 'app.yaml'
    version: str
    db_url: str
    progress_bar_class: Optional[Type]
    data_dir: Path
    championat_config_path: Path
    line4bet_config_path: Path
    fonbet_config_path: Path
    leave_progress_bar: bool
    dry: bool
    log_level: str
    log_file: Optional[Path]
    console_log_format: str
    file_log_format: str
    n_processes: Optional[int]
    table_format: str

    @classmethod
    def _parse_path(cls, source):
        path = Path(source)
        if not path.is_absolute():
            path = Path(__file__).parent.parent / path
        return path

    @classmethod
    def from_yaml(cls, file: TextIO) -> Self:
        data = yaml.safe_load(file)
        match data['progress_bar']:
            case None:
                progress_bar_class = None
            case 'console':
                progress_bar_class = tqdm.tqdm
            case 'notebook':
                progress_bar_class = tqdm.notebook.tqdm
            case progress_bar_data:
                raise ValueError(f'Invalid progress bar "{progress_bar_data}".')
        return cls(
            db_url=data['db'],
            version=data['version'],
            progress_bar_class=progress_bar_class,
            data_dir=cls._parse_path(data['data_dir']),
            championat_config_path=cls._parse_path(data['championat_config']),
            line4bet_config_path=cls._parse_path(data['line4bet_config']),
            fonbet_config_path=cls._parse_path(data['fonbet_config']),
            leave_progress_bar=data['leave_progress_bar'],
            dry=data['dry'],
            log_level=data['log_level'],
            log_file=data['log_file'] and cls._parse_path(data['log_file']),
            file_log_format=data['log_format']['file'],
            console_log_format=data['log_format']['console'],
            n_processes=data['n_processes'],
            table_format=data['table_format'],
        )

    @classmethod
    def get_default(cls) -> Self:
        with open(cls.default_source) as file:
            return cls.from_yaml(file)


default = AppConfig.get_default()
