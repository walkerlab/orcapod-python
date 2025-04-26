
# class GlobStream(FixedStream):
#     def __init__(self, name, file_path, pattern='*'):
#         super().__init__(name)
#         self.file_path = file_path
#         self.pattern = pattern
#         if not self.file_path.endswith('/'):
#             self.file_path += '/'

#     def __iter__(self):
#         for file in glob.glob(self.file_path + self.pattern):
#             yield {name: filed}
