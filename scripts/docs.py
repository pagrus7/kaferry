"""Check shell syntax and local Markdown links without executing examples."""

from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote, urlsplit


def prose_and_shell(text):
    prose, shell = [], []
    fence, language, body = None, '', []
    for line in text.splitlines():
        if fence:
            if re.fullmatch(r' {0,3}' + re.escape(fence[0]) + '{' + str(len(fence)) + r',}\s*', line):
                if language in {'sh', 'bash'}:
                    shell.append('\n'.join(body))
                fence, body = None, []
            else:
                body.append(line)
        else:
            opening = re.fullmatch(r' {0,3}(`{3,}|~{3,})(\w*)\s*', line)
            if opening:
                fence, language = opening.groups()
            else:
                prose.append(line)
    if fence:
        raise ValueError('unclosed code fence')
    return '\n'.join(prose), shell


def anchors(text):
    seen, result = {}, set()
    for title in re.findall(r'^#{1,6}\s+(.+?)(?:\s+#+)?$', text, re.M):
        name = re.sub(r'[^\w\- ]', '', title.lower()).replace(' ', '-')
        count = seen.get(name, 0)
        seen[name] = count + 1
        result.add(f'{name}-{count}' if count else name)
    return result


def check(path):
    prose, shells = prose_and_shell(path.read_text())
    for index, script in enumerate(shells):
        result = subprocess.run(['bash', '-n'], input=script, text=True, capture_output=True)
        if result.returncode:
            raise ValueError(f'shell block {index + 1}: {result.stderr.strip()}')
    for link in re.findall(r'\]\(([^\s)]+)(?:\s+"[^"]*")?\)', prose):
        url = urlsplit(link.strip('<>'))
        if url.scheme or url.netloc:
            continue
        target = path.parent / unquote(url.path) if url.path else path
        if not target.exists():
            raise ValueError(f'local link does not exist: {link}')
        if url.fragment and target.suffix == '.md':
            target_prose, _ = prose_and_shell(target.read_text())
            if unquote(url.fragment) not in anchors(target_prose):
                raise ValueError(f'local heading does not exist: {link}')


if __name__ == '__main__':
    files = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '*.md'], text=True).splitlines()
    errors = []
    for name in files:
        try:
            check(Path(name))
        except ValueError as error:
            errors.append(f'{name}: {error}')
    if errors:
        sys.exit('\n'.join(errors))
    readme = Path('README.md').read_text()
    if len(readme) > 5500:
        sys.exit(f'Documentation budget exceeded: README {len(readme)}/5500')
    print(f'Local links and shell syntax checked in {len(files)} Markdown files.')
