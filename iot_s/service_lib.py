import asyncio
import sys
import time
from azure_service_lib import \
    IoTHubRegistryManager, \
    receive_twin_reported, \
    twin_desired, \
    clear_desired_twin, \
    clear_blob_storage, \
    read_blobs


async def main():
    # Connection to IoTHub
    CONNECTION_STRING_MANAGER = "HostName=iot-mvm111.azure-devices.net;SharedAccessKeyName=serviceConnectiRead;SharedAccessKey=zkdWIyfCqFDfRuel74WND5b8mbULki9u+AIoTChxttQ="
    DEVICE_ID = "test_device"

    iothub_registry_manager = IoTHubRegistryManager(CONNECTION_STRING_MANAGER)

    # Clear the desired twin
    await clear_desired_twin(iothub_registry_manager, DEVICE_ID)

    account_name = 'product1410'
    account_key = 'YU8tv+ZHVdTp+V+Gn9Mh/chmJnJh2cKf7iHRuuOpRlbhpkA7/7x0oNLU22Tj7Hw9K+42CJjYGZBn+AStaj4+4g=='

    STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'

    # Clear the blob storage
    await clear_blob_storage(STORAGE_CONNECTION_STRING)

    old_date_err = ""
    old_date_kpi = ""

    try:
        while True:
            # receive twin reported
            twin_reported = await receive_twin_reported(iothub_registry_manager, DEVICE_ID)

            # sending the twin desired
            await twin_desired(iothub_registry_manager, DEVICE_ID, twin_reported)

            # reading blob storage and actualization date of new blobs
            new_date_err, new_date_kpi = await read_blobs(iothub_registry_manager,
                                                                         DEVICE_ID,
                                                                         STORAGE_CONNECTION_STRING,
                                                                         old_date_err,
                                                                         old_date_kpi)

            # actualization
            old_date_err = new_date_err
            old_date_kpi = new_date_kpi

            time.sleep(1)
    except Exception as e:
        print("Progam is stopeed")
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())