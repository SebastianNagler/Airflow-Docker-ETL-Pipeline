import requests
import logging
from typing import Optional, List, Dict, Any
import pandas as pd
import xml.etree.ElementTree as ET
import os
import boto3
import botocore.exceptions
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_siri_pt_raw_data(api_url: str, api_key: str, s3_client: boto3.client, s3_bucket: str, s3_raw_key: str) -> bool:
    """
    Fetches raw XML data, cleans null bytes, and streams it to S3.
    """
    if not api_key:
        logging.error("API key is missing. Authentication is required.")
        return False

    headers = {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": "SwissTransport-ETL-Pipeline/1.0 (sebastian_nagler@icloud.com)",
        "Accept-Encoding": "gzip, deflate, br"
    }

    try:
        # Uses stream=True to avoid loading the entire response into memory
        with requests.get(api_url, headers=headers, timeout=60, stream=True) as response:
            response.raise_for_status()
            logging.info(f"Successfully connected to {api_url}. Streaming response to S3.")

            # Converts the response into a stream where all null bytes are removed (since null bytes are invalid in XML)
            cleaned_stream = (chunk.replace(b'\x00', b'') for chunk in response.iter_content(chunk_size=8192))
            
            # Uses boto3's upload_fileobj to stream-upload; wraps the generator in a file-like object
            class StreamWrapper:
                """
                Wraps a generator of byte chunks to make it a file-like object
                with a read() method.
                """
                def __init__(self, gen):
                    self.gen = gen
                    self.buffer = b''

                def read(self, size=-1):
                    """
                    Reads bytes. If size == -1, it will read one chunk
                    from the generator. The caller (boto3) is expected
                    to keep calling until it receives an empty byte string.
                    """
                    if size == -1:
                        try:
                            chunk = self.buffer + next(self.gen)
                            self.buffer = b''
                            return chunk
                        except StopIteration:
                            chunk = self.buffer
                            self.buffer = b''
                            return chunk

                    while len(self.buffer) < size:
                        try:
                            self.buffer += next(self.gen)
                        except StopIteration:
                            break

                    chunk = self.buffer[:size]
                    self.buffer = self.buffer[size:]
                    return chunk

            s3_client.upload_fileobj(
                StreamWrapper(cleaned_stream),
                s3_bucket,
                s3_raw_key
            )
            
        logging.info(f"Successfully streamed raw data to s3://{s3_bucket}/{s3_raw_key}")
        return True
    except requests.exceptions.HTTPError as http_err:
        if http_err.response.status_code in [401, 403]:
            logging.error(f"Authentication failed (HTTP {http_err.response.status_code}). Check API key.")
        else:
            logging.error(f"HTTP error occurred: {http_err} (URL: {api_url})")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"An unexpected error occurred: {req_err} (URL: {api_url})")
    except botocore.exceptions.ClientError as e:
        logging.error(f"AWS Client Error during raw data upload: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during extraction: {e}")
    
    return False

def transform_siri_pt_data(s3_bucket: str, s3_raw_key: str, s3_processed_key: str) -> bool:
    """
    Uses iterparse to read the XML from S3 chunk by chunk, and ParquetWriter to write to S3 chunk by chunk.
    """
    
    # Module s3fs enables treating S3 buckets as if they were a local file system
    s3 = s3fs.S3FileSystem()
    s3_raw_path = f"s3://{s3_bucket}/{s3_raw_key}"
    s3_processed_path = f"s3://{s3_bucket}/{s3_processed_key}"

    logging.info(f"Starting S3-to-S3 streaming transform...")
    logging.info(f"Reading from: {s3_raw_path}")
    logging.info(f"Writing to: {s3_processed_path}")

    CHUNK_SIZE = 50000 
    all_stops_data: List[Dict[str, Any]] = []
    siri_schema = pa.schema([
        pa.field('journey_ref', pa.string()),
        pa.field('line_name', pa.string()),
        pa.field('vehicle_mode', pa.string()),
        pa.field('stop_point_ref', pa.string()),
        pa.field('visit_number', pa.int64()),
        pa.field('stop_name', pa.string()),
        pa.field('aimed_arrival_time', pa.timestamp('ns', tz='UTC')),
        pa.field('aimed_departure_time', pa.timestamp('ns', tz='UTC')),
        pa.field('platform', pa.string())
    ])
    
    parquet_writer: Optional[pq.ParquetWriter] = None
    schema_defined = False
    total_records = 0

    try:
        parquet_writer = pq.ParquetWriter(
            s3_processed_path, 
            siri_schema, 
            compression='gzip',
            filesystem=s3
        )
        # 'rb' stands for 'read binary' and is used because the iterparse function is designed to consume a raw byte stream
        with s3.open(s3_raw_path, 'rb') as f_in:
            # Uses iterparse for incremental, low-memory XML parsing
            ns = {'siri': 'http://www.siri.org.uk/siri'}
            context = ET.iterparse(f_in, events=('start', 'end'))

            # The 'root' variable is assigned to clear it later on
            _, root = next(context)

            for event, elem in context:
                if event == 'end' and elem.tag == f"{{{ns['siri']}}}DatedVehicleJourney":
                    
                    journey_ref_elem = elem.find('.//siri:DatedVehicleJourneyRef', ns)
                    line_name_elem = elem.find('.//siri:PublishedLineName', ns)
                    vehicle_mode_elem = elem.find('.//siri:VehicleMode', ns)
                    journey_ref = journey_ref_elem.text if journey_ref_elem is not None else None
                    line_name = line_name_elem.text if line_name_elem is not None else None
                    vehicle_mode = vehicle_mode_elem.text if vehicle_mode_elem is not None else None

                    calls = elem.findall('.//siri:DatedCall', ns)
                    for call in calls:
                        stop_point_ref_elem = call.find('.//siri:StopPointRef', ns)
                        visit_num_elem = call.find('.//siri:VisitNumber', ns)
                        stop_name_elem = call.find('.//siri:StopPointName', ns)
                        arrival_elem = call.find('.//siri:AimedArrivalTime', ns)
                        departure_elem = call.find('.//siri:AimedDepartureTime', ns)
                        platform_elem = call.find('.//siri:DeparturePlatformName', ns)
                        stop_point_ref = stop_point_ref_elem.text if stop_point_ref_elem is not None else None
                        visit_num = int(visit_num_elem.text) if visit_num_elem is not None and visit_num_elem.text is not None else None
                        stop_name = stop_name_elem.text if stop_name_elem is not None else None
                        arrival = arrival_elem.text if arrival_elem is not None else None
                        departure = departure_elem.text if departure_elem is not None else None
                        platform = platform_elem.text if platform_elem is not None else None

                        
                        stop_data = {
                            'journey_ref': journey_ref, 'line_name': line_name, 'vehicle_mode': vehicle_mode,
                            'stop_point_ref': stop_point_ref, 'visit_number': visit_num, 'stop_name': stop_name, 
                            'aimed_arrival_time': arrival, 'aimed_departure_time': departure, 'platform': platform
                        }
                        
                        all_stops_data.append(stop_data)

                    if len(all_stops_data) >= CHUNK_SIZE:
                        logging.info(f"Writing chunk of {len(all_stops_data)} records...")
                        total_records += len(all_stops_data)
                        
                        df_chunk = pd.DataFrame(all_stops_data)
                        df_chunk['aimed_arrival_time'] = pd.to_datetime(df_chunk['aimed_arrival_time'], errors='coerce')
                        df_chunk['aimed_departure_time'] = pd.to_datetime(df_chunk['aimed_departure_time'], errors='coerce')
                        df_chunk['visit_number'] = df_chunk['visit_number'].astype('Int64')

                        df_chunk['journey_ref'] = df_chunk['journey_ref'].astype(pd.StringDtype())
                        df_chunk['line_name'] = df_chunk['line_name'].astype(pd.StringDtype())
                        df_chunk['vehicle_mode'] = df_chunk['vehicle_mode'].astype(pd.StringDtype())
                        df_chunk['stop_point_ref'] = df_chunk['stop_point_ref'].astype(pd.StringDtype())
                        df_chunk['stop_name'] = df_chunk['stop_name'].astype(pd.StringDtype())
                        df_chunk['platform'] = df_chunk['platform'].astype(pd.StringDtype())
                        
                        table = pa.Table.from_pandas(df_chunk, schema=siri_schema, preserve_index=False)
                                          
                        parquet_writer.write_table(table)
                        all_stops_data = []
                    
                    elem.clear()
                    root.clear()

        if all_stops_data:
            logging.info(f"Writing final chunk of {len(all_stops_data)} records...")
            total_records += len(all_stops_data)
            
            df_chunk = pd.DataFrame(all_stops_data)
            df_chunk['aimed_arrival_time'] = pd.to_datetime(df_chunk['aimed_arrival_time'], errors='coerce')
            df_chunk['aimed_departure_time'] = pd.to_datetime(df_chunk['aimed_departure_time'], errors='coerce')
            df_chunk['visit_number'] = df_chunk['visit_number'].astype('Int64')

            df_chunk['journey_ref'] = df_chunk['journey_ref'].astype(pd.StringDtype())
            df_chunk['line_name'] = df_chunk['line_name'].astype(pd.StringDtype())
            df_chunk['vehicle_mode'] = df_chunk['vehicle_mode'].astype(pd.StringDtype())
            df_chunk['stop_point_ref'] = df_chunk['stop_point_ref'].astype(pd.StringDtype())
            df_chunk['stop_name'] = df_chunk['stop_name'].astype(pd.StringDtype())
            df_chunk['platform'] = df_chunk['platform'].astype(pd.StringDtype())
            
            table = pa.Table.from_pandas(df_chunk, schema=siri_schema, preserve_index=False)
            
            parquet_writer.write_table(table)
            
        if total_records == 0:
            logging.warning("Transformation complete, but 0 records were processed.")
        else:
            logging.info(f"Successfully transformed {total_records} records and saved to {s3_processed_path}")
        
        return True

    except (ET.ParseError, IOError, Exception) as e:
        logging.error(f"An error occurred during S3-to-S3 transformation: {e}", exc_info=True)
        if parquet_writer:
            parquet_writer.close()
        return False
    finally:
        if parquet_writer:
            parquet_writer.close()

def get_s3_keys() -> (str, str):
    """Helper function to generate partitioned S3 keys."""
    today = datetime.utcnow()
    s3_raw_key = (
        f"siri_pt_raw/year={today.year}"
        f"/month={today.month:02d}"
        f"/day={today.day:02d}"
        f"/raw_data.xml"
    )
    s3_processed_key = (
        f"siri_pt/year={today.year}"
        f"/month={today.month:02d}"
        f"/day={today.day:02d}"
        f"/siri_pt_data.parquet"
    )
    return s3_raw_key, s3_processed_key

def run_extract():
    """Executes the EXTRACT step."""
    logging.info("Starting Task: EXTRACT")

    try:
        API_KEY = os.environ['API_KEY']
        S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
    except KeyError as e:
        logging.error(f"Environment variable {e} not set.")
        

    SIRI_PT_API_ENDPOINT = "https://api.opentransportdata.swiss/la/siri-pt"
    s3_raw_key, _ = get_s3_keys()
    
    logging.info(f"Raw S3 Key: s3://{S3_BUCKET_NAME}/{s3_raw_key}")
    
    s3_client = boto3.client('s3')

    extract_success = fetch_siri_pt_raw_data(
        SIRI_PT_API_ENDPOINT, API_KEY, s3_client, S3_BUCKET_NAME, s3_raw_key
    )
    
    if not extract_success:
        logging.error("Data extraction failed.")
        raise Exception("Data extraction failed.")
        
    logging.info("EXTRACT task completed successfully.")

def run_transform():
    """Executes the TRANSFORM step."""
    logging.info("Starting Task: TRANSFORM")
    
    try:
        S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
    except KeyError as e:
        logging.error(f"Environment variable {e} not set.")
        raise Exception(f"Environment variable {e} not set.")

    s3_raw_key, s3_processed_key = get_s3_keys()

    logging.info(f"Reading from: s3://{S3_BUCKET_NAME}/{s3_raw_key}")
    logging.info(f"Writing to: s3://{S3_BUCKET_NAME}/{s3_processed_key}")

    transform_success = transform_siri_pt_data(
        S3_BUCKET_NAME, s3_raw_key, s3_processed_key
    )

    if not transform_success:
        logging.error("Data transformation failed.")
        raise Exception("Data transformation failed.")

    logging.info("TRANSFORM task completed successfully.")


if __name__ == "__main__":
    """
    Main entry point.
    Parses command-line arguments to run specific tasks.
    """
    if len(sys.argv) != 2:
        logging.error("Invalid arguments. Usage: python script.py [extract|transform]")
        raise Exception("Invalid arguments. Usage: python script.py [extract|transform]")
        
    task = sys.argv[1]
    
    if task == "extract":
        run_extract()
    elif task == "transform":
        run_transform()
    else:
        logging.error(f"Unknown task: {task}. Use 'extract' or 'transform'.")
        raise Exception(f"Unknown task: {task}. Use 'extract' or 'transform'.")
