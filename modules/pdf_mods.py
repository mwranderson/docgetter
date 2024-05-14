from pypdf import PdfReader, PdfWriter
from fpdf import FPDF

# FPDF subclass with header and footer modifications
class PDF(FPDF):

    def __init__(self, event_title, event_date):
        super().__init__()
        # assign title and date
        # deal with too long event titles
        if len(event_title) > 80:
            event_title = event_title[:77] + '...' 
        self.event_title = event_title
        self.event_date = event_date

    def header(self):
        # Select Arial bold 15
        self.set_font('Arial', 'B', 12)
        # Framed event title
        self.cell(0, 15, txt = self.event_title, border = 1, align='L')
        # Framed date
        self.cell(0, 15, txt = self.event_date, border = 1, align='R')
        # Line break
        self.ln(20)
        
    def footer(self):
        # Go to 1.5 cm from bottom
        self.set_y(-15)
        # Select Arial italic 8
        self.set_font('Arial', size = 12)
        # Print centered page number
        self.cell(0, 10, str(self.page_no()), align='C')


def pdf_splitter(report, file, local_dir = None):
    ''' Old pdf files are saved as bundles. \\ 
    Given report number and address to master pdf bundle file, \\ 
    cuts out that report and returns it as its own PDF.\\
    If given local_dir, operates using local directory'''
    # open master pdf bundle
    if local_dir:
        reader = PdfReader(f'{local_dir}/{file}')
    else:
        reader = PdfReader("/tmp/"+file)
    # get number of pages
    number_of_pages = len(reader.pages)
    # initialize variables needed cut up pages
    break_point = False
    match = False
    page_range = ''
    # go through pages
    for i in range(number_of_pages):
        # exit condition
        if break_point:
            break
        # read page
        page = reader.pages[i]
        # get page text
        text = page.extract_text()
        # get page lines
        lines = text.split('\n')
        # go through lines
        for j, line in enumerate(lines):
            # if report number is found, get page range of report
            # this is typically in the batch pdf's table of contents
            if str(report) in line:
                # line where report number is found
                match = line
                # get that line and the one after 
                # -- page range takes up two lines
                page_range = lines[j+1]
                # set exit condition
                break_point = True
                break
    # if no match found, exit
    if not match:
        return False
    
    # get first page of report
    first_page = int(page_range.split(' ')[0])
    # get last page of report
    last_page = int(page_range.split(' ')[-1])

    # set up pdf object
    writer = PdfWriter()
    # add pages to pdf object
    for i in range(first_page-1, last_page):
        writer.add_page(reader.pages[i])
    # save pdf file to local directory
    if local_dir:
        with open(f"{local_dir}/{report}.pdf", "wb") as fp:
            writer.write(fp)
    else:
        with open(f"/tmp/{report}.pdf", "wb") as fp:
            writer.write(fp)
    
    # return pdf
    return f"{report}.pdf"


def pdf_creator(
        body : str, 
        filename : str, 
        event_title: str, 
        event_date: str, 
        local_dir: str):
    ''' 
    Given transcript body, event_title and event_date, 
    Creates and saves a pdf presenting transcript information. \
    \\
    If local_dir given, saves file to local_dir as filename. Otherwise, saves to /temp.
    '''
    # init pdf
    pdf = PDF(event_title=event_title, event_date=event_date)

    # determine output path
    if local_dir:
        save_dir = f'{local_dir}/{filename}'
    else:
        save_dir = f'/tmp/{filename}'

    # Add a page
    pdf.add_page()
    # set margins
    pdf.set_margins(20, 20, 20)
    
    # set style and size of font 
    # that you want in the pdf
    pdf.set_font("Arial", size = 12)
    
    # replace known unreadable characters
    body = body.replace('\u2019', "'").replace('\u201c', '"').replace('\u201d', "")

    # insert text into pdf
    # note: encoding might add question marks to unknown characters, in which case
    # they must be manually replaced as above.
    pdf.write(h=6, txt=body.encode('latin-1', 'replace').decode('latin-1'))
    
    # save pdf
    pdf.output(save_dir)   

    return filename
