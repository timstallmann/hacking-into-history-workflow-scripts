"""
Works through a list of deed items with identified racial covenants, pulls the corresponding full
text from AWS and searches for strings matching dates, and plat book & page numbers.
"""

import typing
import os
import psycopg
import boto3
import regex
import csv
import itertools

session = boto3.Session(profile_name='dataworks')
s3 = session.client('s3')
S3_BUCKET = 'hih-deeds-textract-output'
DEED_SPLIT_REGEX = regex.compile(r"(?ei)(This (deed|indenture), made){e<2}")
PLAT_REGEX = regex.compile(r"Plat (?ei)(Book (?P<temp>Number |No\.? )?(\d+) (?P<temp2>at )?pages? (\d+)){e<=5}")


class PlatBookPage(typing.TypedDict):
    plat_book: str | None
    plat_page: str | None


class YearConfidence(typing.TypedDict):
    year: str | None
    confidence: float


class DeedRecord(typing.TypedDict):
    filename: str
    covenant_text: str


def get_deeds_with_covenants() -> typing.Generator[DeedRecord, None, None]:
    """
    Query PostGIS DB for item IDs ID'ed as racial covenants
    :return:
    """
    select_query = """
    SELECT d.filename, d.gid, z.covenant_text
        FROM hacking_into_history.deeds d 
        JOIN hacking_into_history.zooniverse_transcriptions__first_round z 
        ON d.gid = z.deeds_gid 
        WHERE z.has_racial_covenant='yes'
    """
    connect_str = f"host={os.getenv('PG_HOST')} password={os.getenv('PG_PASS')} dbname={os.getenv('PG_DB')} user={os.getenv('PG_USER')}"
    with psycopg.connect(connect_str, row_factory=psycopg.rows.dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(select_query)
            for record in cur:
                yield record


def get_textract_transcript(filename: str) -> str:
    """
    Pull the text transcript from AWS for a given item ID
    :param filename: filename <deed book + page>.PDF
    :return: full text of transcript
    """
    response = s3.list_objects(Bucket=S3_BUCKET, Prefix=f'{filename}-analysis/')
    if 'Contents' not in response:
        return ""

    matching_items = [f for f in response['Contents'] if str(f['Key']).endswith('inreadingorder.txt')]

    # Some files were textract-transcribed multiple times, so need to pull just the newest transcription for each page.
    matching_items_dict = {}
    for m in matching_items:
        item_filename = m['Key'].split('/')[-1]
        if item_filename not in matching_items_dict:
            matching_items_dict[item_filename] = [m, ]
        else:
            matching_items_dict[item_filename].append(m)

    matching_items_dict = sorted(matching_items_dict.items(), key=lambda i: i[0])

    return_text = ""
    for k in matching_items_dict:
        matching_key = sorted(k[1], key=lambda i: i['LastModified'])[-1]
        item = s3.get_object(Bucket=S3_BUCKET, Key=matching_key['Key'])
        return_text += item['Body'].read().decode("utf-8").replace("\n", " ").replace("  ", " ")
    return return_text


def find_covenant_deed(deed_item: DeedRecord, deed_text: str | None = None) -> str | bool | None:
    """
    Pulls a deed item from textract and attempts to find within it the
    full text of the deed item matching a specific covenant
    :param deed_item:
    :return:
    """
    if not deed_text:
        deed_text = get_textract_transcript(deed_item['filename'])

    deeds = regex.split(DEED_SPLIT_REGEX, deed_text)
    text_to_match = regex.escape(deed_item['covenant_text'][:80])
    regex_to_match = regex.compile(fr"(?i)({text_to_match}){{e<=6}}")

    matching_deeds = []
    for d in deeds:
        if regex.search(regex_to_match, d):
            matching_deeds.append(d)

    if len(matching_deeds) > 1:
        return False
    if len(matching_deeds) == 0:
        return None
    else:
        return matching_deeds[0]


def find_plat_book_and_page_number(transcript: str) -> PlatBookPage:
    """
    Search through transcript for all strings matching Plat Book __ and Page ___, and
    ID if there is a single unique combination of plat book and page used in the transcript.
    If so, return that, otherwise return false.

    Uses regex package for fuzzy matching, described here:
    https://maxhalford.github.io/blog/fuzzy-regex-matching-in-python/

    :param transcript:
    :return:
    """
    response = {
        'plat_book': None,
        'plat_page': None,
    }
    if not transcript:
        return response

    matches = regex.findall(PLAT_REGEX, transcript)
    plat_books = [regex.search(r"\d+", m[2]) for m in matches]
    plat_books = [m.group(0) for m in plat_books if m]
    plat_pages = [regex.search(r"\d+", m[4]) for m in matches]
    plat_pages = [m.group(0) for m in plat_pages if m]
    plat_books = set(plat_books)
    plat_pages = set(plat_pages)

    if len(plat_books) == 1:
        response['plat_book'] = plat_books.pop()
    if len(plat_pages) == 1:
        response['plat_page'] = plat_pages.pop()

    return response


def parse_years_for_confidence(years: typing.List[str]) -> YearConfidence | None:
    years_set = set(years)
    result = {
        'year': None,
        'confidence': 0
    }

    if len(years) == 0:
        return None

    if len(years) == 1 or len(years_set) == 1:
        result['year'] = years[0]
        result['confidence'] = 1
        return result

    # If there are just two years found and they are consecutive, pull the first year.
    if len(years_set) == 2:
        years_list = sorted(list(years_set))
        if int(years_list[0]) == int(years_list[1]) - 1:
            result['year'] = years_list[0]
            result['confidence'] = 1
            return result

    years_count = len(years)
    years_dict = {}
    for y in years:
        if y not in years_dict:
            years_dict[y] = 0
        years_dict[y] += 1
    sorted_years = sorted(years_dict.items(), key=lambda i: -i[1])
    if sorted_years[0][1] > sorted_years[1][1]:
        result['year'] = sorted_years[0][0]
        result['confidence'] = sorted_years[0][1] / years_count
        return result
    return None


def find_deed_year(transcript: str) -> YearConfidence:
    """
    Search through transcript for all date-like strings, and extract the year.
    If there's a single year or a pair of years separated by only 1, return the
    year (or the first year in the pair). Otherwise, return false.
    :param transcript:
    :return:
    """

    years = regex.findall(r"(19\d\d)(,)? by and between", transcript)
    result = parse_years_for_confidence([y[0] for y in years])
    if result:
        return result

    years = regex.findall(r"19\d\d", transcript)
    result = parse_years_for_confidence(years)
    if result:
        return result

    return {
        'year': None,
        'confidence': 0,
    }


def is_cemetery_deed(transcript: str) -> bool:
    if regex.search(r"(?i)cemetery", transcript) or regex.search(r"(?i)(woodlawn memorial){e<2}", transcript):
        return True
    return False


def write_results_to_csv(filename: str):
    fieldnames = ['deeds_gid', 's3_link', 'is_cemetery_deed', 'year', 'year_confidence', 'plat_book', 'plat_page',
                  'notes']
    with open(filename, "w") as outfile:
        out_csv = csv.DictWriter(outfile, fieldnames)
        out_csv.writeheader()
        for i in get_deeds_with_covenants():
            try:
                deed_text = get_textract_transcript(i['filename'])
                text = find_covenant_deed(i, deed_text)
                year = {
                    'year': None,
                    'confidence': None
                }
                plat_book_page = {
                    'plat_book': None,
                    'plat_page': None,
                }
                cemetery = None
                note = ""
                if text:
                    year = find_deed_year(text)
                    plat_book_page = find_plat_book_and_page_number(text)
                    cemetery = is_cemetery_deed(text)
                elif text == False:
                    note = "Contains multiple deeds!"
                else:
                    note = "No deed found!"
                out_csv.writerow({
                    'deeds_gid': i['gid'],
                    's3_link': f"https://hih-deeds.s3.amazonaws.com/{i['filename']}",
                    'is_cemetery_deed': cemetery,
                    'year': year['year'],
                    'year_confidence': year['confidence'],
                    'plat_book': plat_book_page['plat_book'],
                    'plat_page': plat_book_page['plat_page'],
                    'notes': note,
                })
                print(i['filename'])
            except AttributeError as e:
                print(e)

#
# results = [r for r in itertools.islice(get_deeds_with_covenants(), 25)]
# print(results[18])
# print(find_covenant_deed(results[18]))
# print(results[20])
# print(find_covenant_deed(results[20]))
# print(results[21])
# print(get_textract_transcript(results[21]['filename']))
# print(find_covenant_deed(results[21]))
# print(results[3])
# print(find_covenant_deed(results[3]))
#
# for i in itertools.islice(get_deeds_with_covenants(), 3, 8):
#     print(i)
#     print(find_covenant_deed(i))

# for i in itertools.islice(get_deeds_with_covenants(), 25):
#     deed_text = get_textract_transcript(i['filename'])
#     text = find_covenant_deed(i, deed_text)
#     if text:
#         print(find_plat_book_and_page_number(text))

write_results_to_csv('results_all.csv')
