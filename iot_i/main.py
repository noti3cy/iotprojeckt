import time
import sys
from mach_lib import Machine, asyncio, ua
from asyncua import Client
from device_lib import \
    IoTHubModuleClient, \
    Message, \
    d2c, \
    twin_reported, \
    receive_twin_desired, \
    take_direct_method, \
    send_email, \
    device_errors_compare


async def main():
    opcua_endpoint = "opc.tcp://localhost:4840/"
    CONNECTION_STRING = "HostName=iot-mvm111.azure-devices.net;DeviceId=test_device;SharedAccessKey=pXg/g+4Z5shSuWqKJGcZKRfpHU+vRhUOrAIoTNcsbiU="

    client_opc = Client(opcua_endpoint)

    # Connection to OPC UA server
    try:
        await client_opc.connect()
    except Exception as e:
        print("Not connect to OPC UA")
        print(f"Error: {e}")
        sys.exit(1)
    else:
        print("Successful connection to OPC UA")

    # Connection to IoTHub
    try:
        client_iot = IoTHubModuleClient.create_from_connection_string(CONNECTION_STRING)
        client_iot.connect()
    except Exception as e:
        print("Not connect to IoTHub")
        print(f"Error: {e}")
        sys.exit(1)
    else:
        print("Successful connection to IoTHub")

    # Clean the reported twin
    twin = client_iot.get_twin()['reported']
    del twin["$version"]
    for key, value in twin.items():
        twin[key] = None
    client_iot.patch_twin_reported_properties(twin)

    lst_dev_err_old = []

    try:
        while True:
            lst = await client_opc.get_objects_node().get_children()
            # lst[0] is a server. We don't need it
            lst = lst[1:]

            # list of actual devices
            lst_machines = []

            # actualization
            lst_dev_err_new = []

            # write devices in list and update data
            for i in range(len(lst)):
                machine = Machine(client_opc, lst[i])
                await machine.update_data()
                lst_machines.append(machine)

                lst_dev_err_new.append(machine.device_error)

            await receive_twin_desired(client_iot, lst_machines)

            # print actual data about our devices
            for j in range(len(lst_machines)):
                # print(lst_machines[j])
                await d2c(client_iot, lst_machines[j])
                await twin_reported(client_iot, lst_machines[j])

                # when value changes, a single D2C message must be sent to IoT platform
                if lst_dev_err_old != []:
                    res_comp = device_errors_compare(lst_dev_err_old[j], lst_dev_err_new[j])
                    if res_comp[0]:
                        await d2c(client_iot, lst_machines[j], True)

                        # send email to predefined address
                        await send_email(lst_machines[j], res_comp[1])

            # take a direct methods and call them
            await take_direct_method(client_iot, client_opc)

            # actualization
            lst_dev_err_old = []
            for err in lst_dev_err_new:
                lst_dev_err_old.append(err)

            # optional
            time.sleep(1)
    except KeyboardInterrupt:
        print("Keyboard stopped program")

    # disconnect
    await client_opc.disconnect()
    client_iot.disconnect()


if __name__ == "__main__":
    asyncio.run(main())