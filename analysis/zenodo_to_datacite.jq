# Transforms a Zenodo record into the DataCite-style structure described by datacite_schema.json.
#
# Accepts EITHER shape as input:
#   - the full Zenodo record (has .metadata, .links, .files)  -> url and formats are filled in
#   - just the metadata block (the zenodo_schema.json shape)  -> url and formats are omitted
#
# Usage:  jq -c -f zenodo_to_datacite.jq record.json

def cap: if . == null or . == "" then null else (.[0:1] | ascii_upcase) + .[1:] end;

# drop null values and empty arrays so optional fields vanish instead of failing validation
def clean: with_entries(select(.value != null and .value != []));

def general_type:
  if . == null then "Other"
  elif . == "dataset" then "Dataset"
  elif . == "software" then "Software"
  elif startswith("image") then "Image"
  elif . == "video" then "Audiovisual"
  elif . == "physicalobject" then "PhysicalObject"
  elif startswith("publication") or . == "poster" or . == "presentation" then "Text"
  else "Other" end;

def title_type:
  if . == "alternative-title" then "AlternativeTitle"
  elif . == "subtitle" then "Subtitle"
  elif . == "translated-title" then "TranslatedTitle"
  else "Other" end;

def description_type:
  if . == "abstract" then "Abstract"
  elif . == "methods" then "Methods"
  elif . == "technical-info" then "TechnicalInfo"
  elif . == "seriesinformation" or . == "series-information" then "SeriesInformation"
  elif . == "tableofcontents" or . == "table-of-contents" then "TableOfContents"
  else "Other" end;

def id_scheme: if . == "orcid" then "ORCID" elif . == null then null else ascii_upcase end;

def is_full_date: type == "string" and test("^[0-9]{4}-[0-9]{2}-[0-9]{2}$");

. as $rec
| .metadata as $m
| {
    url: ($rec.links.doi
          // (if $rec.pids.doi.identifier then "https://doi.org/" + $rec.pids.doi.identifier else null end)),
    titles:
      ( [ {title: $m.title} ]
        + [ ($m.additional_titles // [])[]
            | {title: .title, titleType: (.type.id | title_type)} ]
      ),

    creators:
      [ ($m.creators // [])[]
        | .person_or_org
        | select(. != null)
        | {
            creatorName: .name,
            nameType: (.type | cap),
            givenName: .given_name,
            familyName: .family_name,
            nameIdentifiers:
              [ (.identifiers // [])[]
                | {nameIdentifierScheme: (.scheme | id_scheme), nameIdentifier: .identifier}
                | clean ]
          }
        | clean ],
    formats: ([ ($rec.files.entries // {}) | .[] | .mimetype ] | map(select(. != null)) | unique),
    subjects:
      [ ($m.subjects // [])[]
        | {subject: .subject, subjectScheme: .scheme}
        | clean ],

    alternateIdentifiers:
      [ ($m.identifiers // [])[]
        | {alternateIdentifier: .identifier, alternateIdentifierType: .scheme}
        | clean ],

    publicationYear: ((($m.publication_date // "")[0:4]) | if . == "" then null else . end),

    descriptions:
      ( [ if $m.description then {description: $m.description, descriptionType: "Abstract"} else empty end ]
        + [ ($m.additional_descriptions // [])[]
            | {description: .description, descriptionType: (.type.id | description_type)} ]
      ),

    dates:
      ( [ if ($m.publication_date | is_full_date)
          then {date: $m.publication_date, dateType: "Issued"} else empty end ]
        + [ ($m.dates // [])[]
            | select(.date | is_full_date)
            | {date: .date, dateType: ((.type.id | cap) // "Other")} ]
      ),

    types: ({
      resourceType: $m.resource_type.title.en,
      resourceTypeGeneral: ($m.resource_type.id | general_type)
    } | clean),

    rightsList:
      [ ($m.rights // [])[]
        | {rights: .title.en, rightsUri: .props.url}
        | clean ]
  }
| clean
