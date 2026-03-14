from __future__ import annotations

from pathlib import Path

from .learning import AnalysisProfile


def resolve_external_xml_path(
    xml_name: str | None,
    source_path: Path,
    input_dir: Path,
    profile: AnalysisProfile | None,
) -> tuple[Path, str | None, str | None]:
    if not xml_name:
        return source_path.resolve(), None, None

    direct_path = (source_path.parent / xml_name).resolve()
    if direct_path.exists():
        return direct_path, None, None

    if profile:
        source_dir = relative_dir_from_root(source_path.parent.resolve(), input_dir.resolve())
        scoped_key = f"{source_dir}::{xml_name}"
        if scoped_key in profile.external_xml_scoped_map:
            mapped_name = profile.external_xml_scoped_map[scoped_key]
            mapped_path = (input_dir.resolve() / mapped_name).resolve()
            if mapped_path.exists():
                return mapped_path, mapped_name, "external_xml_scoped_mapping"

        if xml_name in profile.external_xml_name_map:
            mapped_name = profile.external_xml_name_map[xml_name]
            mapped_path = (input_dir.resolve() / mapped_name).resolve()
            if mapped_path.exists():
                return mapped_path, mapped_name, "external_xml_name_mapping"

    return direct_path, None, None


def relative_dir_from_root(path: Path, input_dir: Path) -> str:
    relative = path.relative_to(input_dir).as_posix()
    return relative or "."
