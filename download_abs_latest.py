from requests_html import HTMLSession
from pathlib import Path
import time

def get_files(url, make_directories=True):

    session = HTMLSession()

    home_folder = Path('.')

    r = session.get(url)

    pdfs = sorted([s for s in r.html.absolute_links if Path(s).suffix in ['.R', '.Rmd', '.zip', '.pdf']]) #'.html'

    for link in pdfs:
        link_path = Path(link)
        parts = link_path.parts

        if not (home_folder / parts[-2]).is_dir():
            print (f'Make directory {parts[-2]}')
            (home_folder / parts[-2]).mkdir()

        print(link_path.stem)
        r_pdf = session.get(link)

        with open(home_folder.joinpath(*link_path.parts[-2:]),'wb') as f:
            f.write(r_pdf.content)

        time.sleep(1)
    return None
