import json
import csv
import sys


def json_to_csv(filename, outfilename):
    # Use a breakpoint in the code line below to debug your script.
    with open(outfilename, 'w') as outfile:
        output = csv.writer(outfile)
        output.writerow(["Book","Page","Filename","IsMatch","MatchedTerms","MatchedTexts"])
        with open(filename) as inpfile:
            json_data = json.load(inpfile)
            for deed in json_data.get('Items', []):
                matched_terms = "|".join([t.get('S', '') for t in deed.get('matchedTerms', {}).get('L', [])])
                matched_texts = "|".join([t.get('S', '') for t in deed.get('matchedTexts', {}).get('L', [])])
                object_name = deed.get("objectName", {}).get("S", "")
                is_match = deed.get("isMatch", {}).get("BOOL", None)
                output.writerow([object_name[0:4],object_name[4:8],object_name,is_match,matched_terms,matched_texts])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(
            "Usage -- python json_to_csv.py <input JSON filename> <output CSV filename>.\n"
        )
        exit()

    json_to_csv(sys.argv[1], sys.argv[2])

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
