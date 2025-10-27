from src.utils.utils_dataframe import *
from src.utils.utils_general import *
import time
import ast
import requests
import pandas as pd
import logging
from configs import logging_config
import datetime


def eduschool_fetch_attendance_and_marks(token, classes_df, quarters_df, journals_df):
    class_to_journals = {class_id: group['journal_id'].tolist() for class_id, group in journals_df.groupby('class_id')}
    class_ids = classes_df["id"].tolist()

    # filtering active quarter
    # Convert to datetime
    quarters_df["starts_at"] = pd.to_datetime(quarters_df["starts_at_timestamp"], utc=True)
    quarters_df["ends_at"] = pd.to_datetime(quarters_df["ends_at_timestamp"], utc=True)

    # Get current UTC date
    now = datetime.datetime.now(datetime.timezone.utc)

    # Filter only the active quarter (start_date <= now <= end_date)
    active_quarter = quarters_df[(quarters_df["starts_at"] <= now) & (quarters_df["ends_at"] >= now)]

    # If no active quarter, get last finished one ---
    if active_quarter.empty:
        past_df = active_quarter[active_quarter["ends_at"] < now]
        if not past_df.empty:
            # keep only the most recent finished one
            active_quarter = past_df[past_df["ends_at"] == past_df["ends_at"].max()]
        else:
            # fallback: first upcoming quarter if none finished yet
            active_quarter = quarters_df[quarters_df["starts_at"] == quarters_df["starts_at"].min()]

    quarter_ids = active_quarter["id"].tolist()

    # Headers from the example
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'authorization': f'Bearer {token}',
        'branch': '68417f7edbbdfc73ada6ef01',
        'connection': 'keep-alive',
        'host': 'backend.eduschool.uz',
        'language': 'en',
        'organization': 'test',
        'origin': 'https://omonschool.eduschool.uz',
        'referer': 'https://omonschool.eduschool.uz/',
        'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'academicyearid': '6841869b8eb7901bc71c7807'
    }

    # Function to fetch attendance for a class_id, subject_id (journal_id), quarter_id
    def fetch_attendance(quarter_id, class_id, subject_id, timeout_sec=30):
        base_url = 'https://backend.eduschool.uz/moderator-api/attendances/class'
        params_local = {
            'quarterId': quarter_id,
            'classId': class_id,
            'subjectId': subject_id,
            'childId': subject_id
        }
        try:
            response = requests.get(base_url, params=params_local, headers=headers, timeout=timeout_sec)
            response.raise_for_status()
            data = response.json()

            if data['code'] != 0:
                raise ValueError(
                    f"API error for class {class_id}, subject {subject_id}, quarter {quarter_id}: {data['message']}")

            attendance_context = data['data']['data']
            attendances = [att for block in data['data']['data'] for att in block.get('attendances', [])]
            return True, attendance_context, attendances  # Success flag
        except requests.exceptions.Timeout:
            logging.error(
                f"Timeout after {timeout_sec}s for class {class_id}, subject {subject_id}, quarter {quarter_id}")
            return False, [], []  # Failure flag
        except Exception as e:
            logging.error(f"Unexpected error for class {class_id}, subject {subject_id}, quarter {quarter_id}: {e}")
            return False, [], []  # Failure flag, no raise to allow queueing

    all_attendance_context = []
    all_attendances = []
    retry_queue = []  # List to hold failed (quarter_id, class_id, subject_id) tuples

    # Main loop: Initial fetches
    for quarter_id in quarter_ids:
        # if len(all_attendance_context) > 100: break
        for class_id in class_ids:
            # if len(all_attendance_context) > 100: break
            relevant_journal_ids = class_to_journals.get(class_id, [])
            if not relevant_journal_ids:
                logging.info(f"Skipping class {class_id} as it has no associated journals.")
                continue
            for subject_id in relevant_journal_ids:
                # if len(all_attendance_context) > 100: break
                success, attendance_context, attendances = fetch_attendance(quarter_id, class_id, subject_id)
                if success:
                    # Add identifiers and extend
                    for att in attendance_context:
                        att['class_id'] = class_id
                        att['subject_id'] = subject_id
                        att['quarter_id'] = quarter_id
                    all_attendance_context.extend(attendance_context)
                    all_attendances.extend(attendances)
                else:
                    retry_queue.append((quarter_id, class_id, subject_id))

    # Second retry queue: Attempt failed combos again (with optional increased timeout)
    if retry_queue:
        logging.info(f"Retrying {len(retry_queue)} failed fetches...")
        for quarter_id, class_id, subject_id in retry_queue:
            logging.info(
                f"Retry starting for quarter {quarter_id}, class {class_id}, subject {subject_id} at {time.time()}")
            success, attendance_context, attendances = fetch_attendance(quarter_id, class_id, subject_id,
                                                                        timeout_sec=60)  # Increased timeout for retry
            if success:
                # Add identifiers and extend
                for att in attendance_context:
                    att['class_id'] = class_id
                    att['subject_id'] = subject_id
                    att['quarter_id'] = quarter_id
                all_attendance_context.extend(attendance_context)
                all_attendances.extend(attendances)
            else:
                logging.warning(f"Retry failed again for class {class_id}, subject {subject_id}, quarter {quarter_id}")

    # Create DataFrames
    df_attendance_context = pd.DataFrame(all_attendance_context)
    df_attendances = pd.DataFrame(all_attendances)
    df_attendance_context.drop('attendances', axis=1, inplace=True)

    # Flatten 'period' — always keep indices 0–8
    def flatten_period(cell):
        """
        Flatten a period cell into a compact string like:
        'lessonHour:state:_id'
        Args:
            cell: Input cell containing period data (dict or stringified dict).

        Returns:
            str or None: Flattened representation or None if invalid.
        """
        if pd.isna(cell) or cell == '{}':
            return None
        try:
            d = ast.literal_eval(cell) if isinstance(cell, str) else cell
            lesson_hour = d.get('lessonHour')
            state = d.get('state')
            period_id = d.get('_id')
            return f"{lesson_hour}:{state}:{period_id}"
        except Exception:
            return None

    # Flatten 'period' — always keep indices 0–8
    if 'period' in df_attendance_context.columns:
        for i in range(9):  # ✅ fixed indices 0–8
            period_series = df_attendance_context['period'].apply(
                lambda x: x[i] if isinstance(x, list) and i < len(x) else None
            )
            df_attendance_context[f'period__{i}'] = period_series.apply(flatten_period)
        # Drop the original nested column
        df_attendance_context.drop(columns='period', inplace=True)

    # Flatten 'markHistory' — always keep indices 0–8
    if 'markHistory' in df_attendances.columns:
        for i in range(9):  # ✅ fixed indices 0–8
            mark_series = df_attendances['markHistory'].apply(
                lambda x: x[i] if isinstance(x, list) and i < len(x) else None
            )
            df_attendances[f'markHistory__{i}'] = mark_series

        # Drop the original nested column
        df_attendances.drop(columns='markHistory', inplace=True)

    def flatten_mark_history(cell):
        if pd.isna(cell) or cell == '{}':
            return None
        try:
            d = ast.literal_eval(cell) if isinstance(cell, str) else cell
            date_short = d.get('date', '')[:10]  # keep only YYYY-MM-DD
            return f"{d.get('markSetByEmployeeId')}:{date_short}:{d.get('oldMark')}:{d.get('newMark')}:{d.get('newComment')}"
        except Exception:
            return None

    # apply to all columns matching pattern
    cols = [c for c in df_attendances.columns if c.startswith('markHistory_')]
    cols_per = [c for c in df_attendance_context.columns if c.startswith('period_')]
    df_attendances[cols] = df_attendances[cols].map(flatten_mark_history).fillna('NA').astype(str)
    df_attendance_context[cols_per] = df_attendance_context[cols_per].map(flatten_period).fillna('NA').astype(str)

    df_attendance_context.fillna(0, inplace=True)
    df_attendance_context = clean_string_columns(df_attendance_context)
    df_attendance_context = normalize_columns(df_attendance_context)
    df_attendance_context = add_timestamp(df_attendance_context)

    df_attendances.fillna(0, inplace=True)
    df_attendances = clean_string_columns(df_attendances)
    df_attendances = normalize_columns(df_attendances)
    df_attendances = add_timestamp(df_attendances)

    df_attendance_context.attrs["name"] = "education_attendance_context"
    df_attendances.attrs["name"] = "education_attendances"

    df_attendance_context["homework"] = 'NA'

    # Save dfs to CSV
    save_df_with_timestamp(df=df_attendance_context)
    save_df_with_timestamp(df=df_attendances)

    return df_attendance_context, df_attendances


def eduschool_fetch_classes(token):
    # API endpoint and base params
    base_url = 'https://backend.eduschool.uz/moderator-api/class/pagin'
    params = {
        'limit': 20,  # As per the example; can adjust if needed
        'headTeachersIds': '[]',
        'search': ''
    }
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'authorization': f'Bearer {token}',
        'branch': '68417f7edbbdfc73ada6ef01',
        'connection': 'keep-alive',
        'host': 'backend.eduschool.uz',
        'language': 'en',
        'organization': 'test',
        'origin': 'https://omonschool.eduschool.uz',
        'referer': 'https://omonschool.eduschool.uz/',
        'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'academicyearid': '6841869b8eb7901bc71c7807'
    }

    # Function to fetch all pages with pagination
    def fetch_all_classes():
        all_classes = []
        page = 1
        total = None

        while True:
            params['page'] = page
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()  # Raise error if not 200
            data = response.json()

            if data['code'] != 0:
                raise ValueError(f"API error: {data['message']}")

            classes = data['data']['data']
            all_classes.extend(classes)

            if total is None:
                total = data['data']['total']

            if len(all_classes) >= total:
                break

            page += 1

        return all_classes

    # Fetch data
    classes_data = fetch_all_classes()

    # Create initial DataFrame
    df = pd.DataFrame(classes_data)

    # Delete moderators entirely (drop column if exists, no flattening)
    if 'moderators' in df.columns:
        df.drop('moderators', axis=1, inplace=True)

    # Flatten building (dict with potential nested list like turnstiles)
    if 'building' in df.columns:
        building_df = pd.json_normalize(df['building'], sep='_')
        df = pd.concat([df.drop('building', axis=1), building_df.add_prefix('building_')], axis=1)

    if 'headTeacher' in df.columns:
        # Extract first headTeacher dict safely
        df['headTeacher'] = df['headTeacher'].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None
        )

        # Flatten into separate columns
        headTeacher_df = pd.json_normalize(df['headTeacher'], sep='_')

        # Merge back
        df = pd.concat(
            [df.drop('headTeacher', axis=1), headTeacher_df.add_prefix('headTeacher_')],
            axis=1
        )
    df = clean_string_columns(df)
    df = normalize_columns(df)
    df = add_timestamp(df)

    df[['uuid', 'grade']] = df[['uuid', 'grade']].apply(lambda s: fill_and_numeric(s, dtype="int"))
    df[['students_count', 'max_students_count']] = df[['students_count', 'max_students_count']].apply(
        lambda s: fill_and_numeric(s, dtype="int"))
    df = df.rename(columns={"letter": "section", "language": "instruction_language"})

    logging.info("Eduschool. Classes have been fetched.")

    df.attrs["name"] = "education_classes"
    save_df_with_timestamp(df=df)

    return df


def eduschool_fetch_employees(token):
    # API endpoint and base params
    base_url = 'https://backend.eduschool.uz/moderator-api/employees/pagin'
    params = {
        'limit': 200,  # As per the example; can adjust if needed
    }
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'identity',
        'accept-language': 'en-US,en;q=0.9',
        'authorization': f'Bearer {token}',
        'branch': '68417f7edbbdfc73ada6ef01',
        'connection': 'keep-alive',
        'host': 'backend.eduschool.uz',
        'language': 'en',
        'organization': 'test',
        'origin': 'https://omonschool.eduschool.uz',
        'referer': 'https://omonschool.eduschool.uz/',
        'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'academicyearid': '6841869b8eb7901bc71c7807'
    }

    # Function to fetch all pages with pagination
    def fetch_all_employees():
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()  # Raise error if not 200
        data = response.json()

        # Check success instead of code, per preview format
        if not data.get('success', False):
            raise ValueError(f"API error: {data.get('message', 'Unknown error')}")

        # Access data per preview: data['data']['data'] for employees, data['data']['total'] for total
        employees = data['data']['data'] if 'data' in data['data'] else data['data']  # Handle potential wrapper

        return employees

    # Fetch data
    employees_data = fetch_all_employees()

    # Create initial DataFrame
    df = pd.DataFrame(employees_data)

    # Flatten branchEmployee (dict)
    if 'branchEmployee' in df.columns:
        branch_df = pd.json_normalize(df['branchEmployee'], sep='__')
        df = pd.concat([df.drop('branchEmployee', axis=1), branch_df.add_prefix('branchEmployee__')], axis=1)

    # Flatten employeeSubjects (list)
    if 'employeeSubjects' in df.columns:
        max_subjects = df['employeeSubjects'].str.len().max() if not df['employeeSubjects'].isnull().all() else 0
        for i in range(max_subjects):
            df[f'employeeSubjects__{i}'] = df['employeeSubjects'].str[i]
        df.drop('employeeSubjects', axis=1, inplace=True)

    # Flatten subjects (list)
    if 'subjects' in df.columns:
        max_subjects = df['subjects'].str.len().max() if not df['subjects'].isnull().all() else 0
        for i in range(max_subjects):
            df[f'subjects__{i}'] = df['subjects'].str[i]
        df.drop('subjects', axis=1, inplace=True)

    # Flatten customFields if present (list)
    if 'customFields' in df.columns:
        df.drop('customFields', axis=1, inplace=True)  # Drop if empty/irrelevant

    # Clean and enrich dfs
    df.fillna(0, inplace=True)
    df = clean_string_columns(df)
    df = normalize_columns(df)
    df = add_timestamp(df)

    df.attrs["name"] = "education_employees"

    # Save df to CSV
    save_df_with_timestamp(df=df)

    return df


def eduschool_fetch_journals(token , classes_df):
    class_ids = classes_df["id"].tolist()

    # Headers from the example
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'authorization': f'Bearer {token}',
        'branch': '68417f7edbbdfc73ada6ef01',
        'connection': 'keep-alive',
        'host': 'backend.eduschool.uz',
        'language': 'en',
        'organization': 'test',
        'origin': 'https://omonschool.eduschool.uz',
        'referer': 'https://omonschool.eduschool.uz/',
        'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'academicyearid': '6841869b8eb7901bc71c7807'
    }

    # Function to fetch journal for a single class ID (no pagination, single request)
    def fetch_journal_for_class(class_id):
        base_url = f'https://backend.eduschool.uz/moderator-api/journal/{class_id}'
        params_local = {
            'search': '',
            'limit': 20,
            'page': 1
        }
        response = requests.get(base_url, params=params_local, headers=headers)
        response.raise_for_status()
        data = response.json()

        if data['code'] != 0:
            raise ValueError(f"API error for class {class_id}: {data['message']}")

        journals = data['data']

        # Add class_id to each journal entry
        for journal in journals:
            journal['class_id'] = class_id

        return journals

    # Loop over class IDs, fetch journals, and collect data
    all_data = []
    for class_id in class_ids:
        try:
            journals = fetch_journal_for_class(class_id)
            all_data.extend(journals)
        except Exception as e:
            print(f"Error for class {class_id}: {e}")

    # Create DataFrame
    df = pd.DataFrame(all_data)

    # Extract and flatten only specified columns
    extracted_data = []

    for _, row in df.iterrows():
        entry = {
            'class_id': row.get('classId'),
            'subject_id': row['subject']['_id'] if 'subject' in row else None,
            'subject_name': row['subject']['name'] if 'subject' in row else None,
            'journal_id': row.get('_id')
        }

        # Flatten teacher IDs (always create columns 0–5)
        for i in range(6):  # ✅ fixed indices 0–5
            col_name = f'teacher_{i}_id'
            if 'teacher' in row and isinstance(row['teacher'], list) and i < len(row['teacher']):
                teacher_item = row['teacher'][i]
                entry[col_name] = teacher_item.get('_id') if isinstance(teacher_item, dict) else None
            else:
                entry[col_name] = None  # ensure column exists even if no data
        # Append result
        extracted_data.append(entry)

    # Create final DataFrame with only specified columns
    df_final = pd.DataFrame(extracted_data)
    cols = [c for c in df_final.columns if c.startswith('teacher_')]
    df_final[cols] = df_final[cols].fillna('NA').astype(str)
    df_final.fillna(0, inplace=True)
    df_final = clean_string_columns(df_final)
    df_final = normalize_columns(df_final)
    df_final = add_timestamp(df_final)

    # Save df to CSV
    df_final.attrs["name"]="education_journals"
    save_df_with_timestamp(df=df_final)

    return df_final


def eduschool_fetch_quarters(token):
    # API endpoint and base params
    base_url = 'https://backend.eduschool.uz/moderator-api/quarter'
    params = {
        'search': '',
        'limit': 200,  # As per the example; sufficient for small totals
    }
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'authorization': f'Bearer {token}',
        'branch': '68417f7edbbdfc73ada6ef01',
        'connection': 'keep-alive',
        'host': 'backend.eduschool.uz',
        'language': 'en',
        'organization': 'test',
        'origin': 'https://omonschool.eduschool.uz',
        'referer': 'https://omonschool.eduschool.uz/',
        'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'academicyearid': '6841869b8eb7901bc71c7807'
    }

    # fetch all quarters
    params['page'] = 1
    response = requests.get(base_url, params=params, headers=headers)
    response.raise_for_status()  # Raise error if not 200
    data = response.json()

    if data['code'] != 0:
        raise ValueError(f"API error: {data['message']}")

    quarters = data['data']

    df = pd.DataFrame(quarters)

    df.fillna(0, inplace=True)
    df = clean_string_columns(df)
    df = normalize_columns(df)
    df = add_timestamp(df)

    df.attrs["name"] = "education_quarters"
    # Save dfs to CSV
    save_df_with_timestamp(df=df)

    return df


def eduschool_fetch_students(token):
    # API endpoint and base params (no filters for full list)
    base_url = 'https://backend.eduschool.uz/moderator-api/students/pagin'
    params = {
        'limit': 20,  # As per the example; can adjust for efficiency (e.g., 200)
        'grade': '[]',
        'search': ''
    }
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'authorization': f'Bearer {token}',
        'branch': '68417f7edbbdfc73ada6ef01',
        'connection': 'keep-alive',
        'host': 'backend.eduschool.uz',
        'language': 'en',
        'organization': 'test',
        'origin': 'https://omonschool.eduschool.uz',
        'referer': 'https://omonschool.eduschool.uz/',
        'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'academicyearid': '6841869b8eb7901bc71c7807'
    }

    all_students = []
    aggregates = {}  # To store totalBalance, totalDebted, totalOwned (from first response)
    page = 1
    total = None

    while True:
        params['page'] = page
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()  # Raise error if not 200
        data = response.json()

        if data['code'] != 0:
            raise ValueError(f"API error: {data['message']}")

        students = data['data']['data']
        all_students.extend(students)

        if total is None:
            total = data['data']['total']
            aggregates = {
                'totalBalance': data['data'].get('totalBalance', 0),
                'totalDebted': data['data'].get('totalDebted', 0),
                'totalOwned': data['data'].get('totalOwned', 0)
            }

        if len(all_students) >= total:
            break

        page += 1

    # Create initial DataFrame for students
    df = pd.DataFrame(all_students)

    # Flatten class (dict) - keep only '_id'
    if 'class' in df.columns:
        class_df = pd.json_normalize(df['class'], sep='__')[['_id']]
        df = pd.concat([df.drop('class', axis=1), class_df.add_prefix('class__')], axis=1)

    # Flatten parents (list of dicts) — always create '_id' and 'type' columns for indices 0–5
    if 'parents' in df.columns:
        for i in range(4):  # ✅ fixed indices 0–3
            parent_series = df['parents'].apply(
                lambda x: x[i] if isinstance(x, list) and i < len(x) else {}
            )

            parent_df = pd.json_normalize(parent_series, sep='__')

            # Ensure both columns exist even if missing in data
            for col in ['_id', 'type']:
                if col not in parent_df.columns:
                    parent_df[col] = None

            parent_df = parent_df[['_id', 'type']].add_prefix(f'parents__{i}__')
            df = pd.concat([df, parent_df], axis=1)

        # Drop original nested column
        df.drop(columns='parents', inplace=True)
        cols = [c for c in df.columns if c.startswith('parents_')]
        df[cols] = df[cols].fillna('NA').astype(str)

    # Flatten status (dict) - keep only '_id', 'name', 'state', 'isDefault'
    if 'status' in df.columns:
        status_df = pd.json_normalize(df['status'], sep='__')[['_id', 'name', 'state', 'isDefault']]
        df = pd.concat([df.drop('status', axis=1), status_df.add_prefix('status__')], axis=1)

    # Flatten subscription (dict) - keep all subfields
    if 'subscription' in df.columns:
        sub_df = pd.json_normalize(df['subscription'], sep='__')
        df = pd.concat([df.drop('subscription', axis=1), sub_df.add_prefix('subscription__')], axis=1)

    # Flatten customFields and otherPhoneNumbers if present (lists, often empty)
    if 'customFields' in df.columns:
        df.drop('customFields', axis=1, inplace=True)  # Drop if empty/irrelevant
    if 'otherPhoneNumbers' in df.columns:
        max_phones = df['otherPhoneNumbers'].str.len().max() if not df['otherPhoneNumbers'].isnull().all() else 0
        for i in range(max_phones):
            df[f'otherPhoneNumbers__{i}'] = df['otherPhoneNumbers'].str[i]
        df.drop('otherPhoneNumbers', axis=1, inplace=True)

    # Create summary DataFrame for aggregates
    agg_df = pd.DataFrame([aggregates])

    # Clean and enrich dfs
    df.fillna(0, inplace=True)
    df = clean_string_columns(df)
    df = normalize_columns(df)
    df = add_timestamp(df)
    agg_df = normalize_columns(agg_df)
    agg_df = add_timestamp(agg_df)
    agg_df['id'] = 1

    agg_df.attrs["name"] = "education_students_aggregated"
    df.attrs["name"] = "education_students"

    df.drop(columns=["final_charge_date_by_subscription", "subscriptoin_stared_charging_at", "last_charged_at",
                     "next_charge_date"], errors="ignore", inplace=True)

    # Save dfs to CSV
    save_df_with_timestamp(df=df)
    save_df_with_timestamp(df=agg_df)

    return df, agg_df




