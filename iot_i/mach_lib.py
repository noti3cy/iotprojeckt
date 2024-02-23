import asyncio
from asyncua import Client, ua


class Machine:

    # intialization
    def __init__(self, client, node):

        self.client = client
        self.node = node
        self.production_status = None
        self.workorder_id = None
        self.production_rate = None
        self.good_count = None
        self.bad_count = None
        self.temperature = None
        self.device_error = None

    # device actualization
    async def update_data(self):
        self.production_status = await self.client.get_node(f"{self.node}/ProductionStatus").get_value()
        self.workorder_id = await self.client.get_node(f"{self.node}/WorkorderId").get_value()
        self.production_rate = await self.client.get_node(f"{self.node}/ProductionRate").get_value()
        self.good_count = await self.client.get_node(f"{self.node}/GoodCount").get_value()
        self.bad_count = await self.client.get_node(f"{self.node}/BadCount").get_value()
        self.temperature = await self.client.get_node(f"{self.node}/Temperature").get_value()
        err_num = await self.client.get_node(f"{self.node}/DeviceError").get_value()
        err_bin_str = bin(err_num)[2:].zfill(4)
        self.device_error = [int(i) for i in err_bin_str]

    # use for print device
    def __str__(self):
        return (f'''
        {str(self.node)[7:]}:
        Production status: {self.production_status}
        Wororder id: {self.workorder_id}
        Production rate: {self.production_rate}
        Good count: {self.good_count}
        Bad count: {self.bad_count}
        Temperature: {self.temperature}
        Device error: {self.device_error}
                    ''')

    # direct method
    async def emergency_stop(self):

        nodeES = self.client.get_node(f"{self.node}/EmergencyStop")
        await self.node.call_method(nodeES)

    # direct method
    async def reset_err_status(self):

        nodeRES = self.client.get_node(f"{self.node}/ResetErrorStatus")
        await self.node.call_method(nodeRES)

    # direct method
    async def set_prod_rate(self, value=10):

        await self.client.set_values([self.client.get_node(f"{self.node}/ProductionRate")],
                                [ua.DataValue(ua.Variant(int(self.production_rate - value), ua.VariantType.Int32))])

