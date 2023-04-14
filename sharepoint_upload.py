import os
from pathlib import Path
import uuid
import logging
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.http.http_method import HttpMethod

import requests


def sharepoint_upload(blob_path: Path, filename: str, sharepoint_folder: str):
    '''
    uploads a file to Sharepoint with support for chunked uploads using the office365-rest-api. 
    There is a Microsoft hard-limit of 262.114 MB on files uploaded to Sharepoint
    using the office365-python-rest-client upload_file method. this sharepoint_upload function 
    implements chunked uploads using office-365-python-rest-client.
    see https://pypi.org/project/Office365-REST-Python-Client/
    '''
    filesize = os.path.getsize(blob_path)
    if filesize > 262_000_000:
        logging.warning(f'{blob_path} / {filename} file too big for upload. Trying chunked uploads!')
        sharepoint_upload_chunked(blob_path, filename, sharepoint_folder, 262_144)
        print(f'Copied {blob_path} to {sharepoint_folder} as {filename}.')
    else:
        #uploads a file to sharepoint the conventional way for files less than 262.144 MB
        ctx = ClientContext(URL).with_credentials(ClientCredential(CLIENT_ID, CLIENT_SECRET))
        target_folder = ctx.web.get_folder_by_server_relative_url(sharepoint_folder)
        with open(blob_path, 'rb') as blob:
            target_folder.upload_file(filename, blob)
            ctx.execute_query()
        print(f'Copied {blob_path} to {sharepoint_folder} as {filename}.')

def sharepoint_upload_chunked(blob_path: Path, filename: str, sharepoint_folder: str, chunk_size: int):
    '''
        input:
        blob_path : path to binary file to upload
        filename : filename you want to name the upload
        sharepoint_folder : the name of the folder you want to upload to
        chunk_size : size of the chunks in bytes. Used in recursion.
    '''
    #log in
    ctx = ClientContext(URL).with_credentials(
                        ClientCredential(CLIENT_ID, CLIENT_SECRET))

    with open(blob_path, 'rb') as f:
        first_chunk = True
        size_previous_chunk = 0
        offset = 0
        filesize = os.path.getsize(blob_path)
        # 31 is just the length of our site's URL

        URL = https://myorg.sharepoint.com/sites/myapp
        #take url after "sites". You are already logged in to myorg.sharepoint.com via ctx (context)
        file_url = URL[29:] + f"/{sharepoint_folder}" + filename
        sharepoint_folder_long = url[31:] + f"/{sharepoint_folder}"
        #each upload needs a guid. You will reference this guid as you upload.
        upload_id = uuid.uuid4()

        #consume the data in chunks.
        while chunk := f.read(chunk_size):
            progressbar(offset, filesize, 30,'■')

            #start upload
            if first_chunk:
                print("adding empty file")
                endpoint_url = f"{url}/_api/web/getfolderbyserverrelativeurl('{sharepoint_folder_long}')/files/add(url='{filename}', overwrite=true)"
                print(f"endpoint url: {endpoint_url}")
                upload_data(ctx, endpoint_url, bytes())
                endpoint_url = f"{url}/_api/web/getfilebyserverrelativeurl('{file_url}')/startupload(uploadID=guid'{upload_id}')"
                response = upload_data(ctx, endpoint_url, chunk)
                first_chunk=False
                
            #Finish upload. if the current chunk is smaller than the previous chunk, it must be the last chunk. 
            elif len(chunk) < size_previous_chunk:
                endpoint_url = f"{url}/_api/web/getfilebyserverrelativeurl('{file_url}')/finishupload(uploadID=guid'{upload_id}',fileOffset={offset})"
                progressbar(filesize, filesize, 30,'■')
                response = upload_data(ctx, endpoint_url, chunk)
                print(response)
            
            #continue upload.
            else :
                #continue to consume the chunks and upload.
                endpoint_url = f"{url}/_api/web/getfilebyserverrelativeurl('{file_url}')/continueupload(uploadID=guid'{upload_id}',fileOffset={offset})"
                response = upload_data(ctx, endpoint_url, chunk)
            
            #length in characters, not in bytes)
            size_previous_chunk = len(chunk)
            offset = offset + size_previous_chunk 
            
            
    #!/usr/bin/python3
def progressbar(current_value,total_value,bar_lengh,progress_char): 
    percentage = int((current_value/total_value)*100)                                                # Percent Completed Calculation 
    progress = int((bar_lengh * current_value ) / total_value)                                       # Progress Done Calculation 
    loadbar = "Progress: [{:{len}}]{}%".format(progress*progress_char,percentage,len = bar_lengh)    # Progress Bar String
    print(loadbar, end='\r')                                                                         # Progress Bar Output
        
def upload_data(ctx : ClientContext, endpoint_url, payload):
    request = RequestOptions(endpoint_url)
    request.set_header('content-lenght', str(len(payload)))
    request.set_header('content-type', "application/json;odata=verbose")
    request.data = payload
    request.proxy = 'yourproxy.net:8080'
    request.method=HttpMethod.Post
    response = ctx.pending_request().execute_request_direct(request)

    try:
        assert response.status_code == 200
    except AssertionError as e:
        logging.error(f'response was not 200, {response.text}')
        raise requests.exceptions.HTTPError({f"{response.status_code}, {response.text}"})
    return response
