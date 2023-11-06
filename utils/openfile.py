import click

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
def open_file(file_path):
    """Open a file in the terminal."""
    click.echo(f"Opening file: {file_path}")
    with open(file_path, 'r') as file:
        content = file.read()
        click.echo(content)

if __name__ == '__main__':
    open_file()
