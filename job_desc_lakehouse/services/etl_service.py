class ETLService:
    def run(self):
        df = self.exact()

        new_df = self.transform(df)

        self.load(new_df)

        return new_df

    def exact(self):
        raise NotImplementedError

    def transform(self, df):
        raise NotImplementedError

    def load(self, df):
        raise NotImplementedError
