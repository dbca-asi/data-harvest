class DataNotExist(Exception):
    def __init__(self,message="Data doesn't exist in database"):
        super().__init__(message)
