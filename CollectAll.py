from pyflink.datastream.functions import WindowFunction


class CollectAll(WindowFunction):
    def apply(self, key, window, inputs): # Тільки 3 аргументи (self, key, window, inputs)
        yield {"metric": key, "data": list(inputs)}