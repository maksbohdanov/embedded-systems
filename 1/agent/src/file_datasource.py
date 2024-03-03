from csv import reader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking
from domain.aggregated_data import AggregatedData
import config

class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
        parking_filename: str,
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.accelerometer_file = None
        self.gps_file = None
        self.parking_file = None

    def read(self) -> AggregatedData:
        """Метод повертає дані отримані з датчиків"""
        accelerometer_data = self.read_data(self.accelerometer_file, self.parse_accelerometer_data)
        gps_data = self.read_data(self.gps_file, self.parse_gps_data)
        parking_data = self.read_data(self.parking_file, self.parse_parking_data)

        return AggregatedData(
            accelerometer=accelerometer_data,
            gps=gps_data,
            parking=parking_data,
            timestamp=datetime.now(),
            user_id=config.USER_ID,
        )


    def startReading(self):
        """Метод повинен викликатись перед початком читання даних"""
        self.accelerometer_file = open(self.accelerometer_filename, 'r')
        next(self.accelerometer_file)
        self.gps_file = open(self.gps_filename, 'r')
        next(self.gps_file)
        self.parking_file = open(self.parking_filename, 'r')
        next(self.parking_file)

    def stopReading(self):
        """Метод повинен викликатись для закінчення читання даних"""
        for file in [self.accelerometer_file, self.gps_file, self.parking_file]:
            if file:
                file.close()

    def read_data(self, file, parser_function):
        csv_reader = reader(file)
        try:
            row = next(csv_reader)
        except StopIteration:
            file.seek(0)
            next(file)
            row = next(csv_reader)

        return parser_function(row)

    def parse_accelerometer_data(self, row) -> Accelerometer:
        x, y, z = map(int, row)
        return Accelerometer(x, y, z)

    def parse_gps_data(self, row) -> Gps:
        longitude, latitude = map(float, row)
        return Gps(longitude, latitude)

    def parse_parking_data(self, row) -> Parking:
        empty_count, longitude, latitude = map(float, row)
        return Parking(int(empty_count), Gps(longitude, latitude))
