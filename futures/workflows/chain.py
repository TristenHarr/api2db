# TODO: Chain class
"""
The Chain class will support chaining multiple Digests together.

The Chain feature should have support to be placed ANYWHERE within the ApiForm and chain together DigestFeatures

A single Digest Feature should run within the same thread/process and be relatively simple.

A series of Digest Features, or features that take more time should be placed into a chain, and chains should support
the ability to:

                Run concurrently i.e. Support both -> ProcessPool/ThreadPool.
                        with Pool(5) as p:
                            res = p.map(DigestFeature, [df_1, df_2, df_3]))

                Run serially: Support both -> Thread/Process

                        def mapper(f, df):
                            do stuff to df
                            return df

                        mapping = [f1, f2, f2]

                        def p_map(fs, df):
                            Process(target=f1, args=(df,))
                            df = Process result
                            Process(target=f2, args=(df,))
                            .
                            .
                            .

Chain example:


c = Chain(
            DigestGetLatLongs(**kwargs),
            DigestApiCallToGetCounty(**kwargs),
            DigestGenerateMapWithLatLongAsPng(**kwargs),
            DigestPostPngToGcs(**kwargs)
            .
            .
            etc.
        )
"""