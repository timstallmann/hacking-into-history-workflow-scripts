import csv
import os
import re
import sys
import subprocess

deed_images_url = "https://hih-deeds-images.s3.amazonaws.com/"
deed_text_path = ""
pdf_directory = "deed-pdfs"


def postgis_to_zooniverse(input_file_name, base_output_file_name, subject_set_id):
    output_deeds = []
    base_file_path = os.path.dirname(input_file_name)
    max_page_count = 1

    with open(input_file_name, 'r') as input_file:
        csv_input = csv.DictReader(input_file)
        for row in csv_input:
            filename = row['filename']
            full_text = ""
            pdf_info_output = subprocess.check_output(['pdfinfo', os.path.join(base_file_path, 'deed-pdfs', filename)], text=True)
            pages = re.search(r"Pages:\s+(\d+)\b", pdf_info_output)
            if not pages:
                print(f"Error: pdf info returned invalid value {pages} for {filename}")
                continue
            pages = int(pages.group(1))
            if pages > max_page_count:
                max_page_count = pages

            with open(os.path.join(base_file_path,'full-text', f"{filename}.txt")) as full_text_file:
                full_text = full_text_file.read()

            deed = {
                    'Full_text': full_text,
                    '!pages': pages,
                    'original_name': filename,
                    '!Matched_terms': row['matched_terms'],
                    '!Deed_book': row['deed_book'],
                    '!Deed_page': row['deed_page']
                }
            base_filename = os.path.splitext(filename)[0]
            for i in range(1, pages + 1):
                deed[f'Filename_{i}'] = f"{deed_images_url}{base_filename}-{i}.png"

            output_deeds.append(deed)

    for p in range(1, max_page_count + 1):
        deeds_with_page_count = [d for d in output_deeds if d['!pages'] == p]
        if not deeds_with_page_count:
            continue

        output_file_name = f'{base_output_file_name}--{p}-pages.csv'
        with open(os.path.join(base_file_path, output_file_name), 'w') as output_file:
            fieldnames = [f"Filename_{n}" for n in range(1, p + 1)] + \
                         ["original_name", "!Matched_terms",
                          "!pages", "Full_text",
                          "!Deed_book", "!Deed_page"]
            output_csv = csv.DictWriter(output_file, fieldnames)
            output_csv.writeheader()
            output_csv.writerows(deeds_with_page_count)

        print(subprocess.check_output(
            ['panoptes', 'subject-set', 'upload-subjects'] +
            [f'-r {n}' for n in range(1, p + 1)] +
            [subject_set_id, os.path.join(base_file_path, output_file_name)]))


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print(
            "Usage -- python postgis_to_zooniverse.py <input CSV filename> <base output name> <subject-set-id>.\n"
            "Input CSV file must have filename, deed_book, deed_page, matched_terms columns."
        )
        exit()

    postgis_to_zooniverse(sys.argv[1], sys.argv[2], sys.argv[3])

