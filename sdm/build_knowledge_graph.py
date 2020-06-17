import os
import multiprocessing


def flattern_mappings():
    flat_structure = []
    for key, value in mapping.items():
        index = 0
        for chunk in mapping[key]:
            flat_structure.append((key, chunk, index))
            index += 1

    return flat_structure


def job(meta):
    content = None
    mapper = meta[0]
    date = meta[1][0]
    chunk = meta[1][1]
    index = meta[2]

    with open(f'mappings/{mapper}', 'r') as content_file:
        content = content_file.read()
        content = content.replace('%yield%', f'resources/sources/{date}/{chunk}')
        content = content.replace('%date%', f'{date}')
        content = content.replace('%file%', f'{chunk}')
    with open(f'resources/tmp/{os.path.splitext(mapper)[0]}-{index}-templated.yml', 'w') as file:
        file.write(content)

    os.system(f'yarrrml-parser -i resources/tmp/{os.path.splitext(mapper)[0]}-{index}-templated.yml -o graph/{os.path.splitext(mapper)[0]}-{index}-rules.rml.ttl')
    os.system(f'java -jar resources/rmlmapper-4.8.0-r248.jar -m graph/{os.path.splitext(mapper)[0]}-{index}-rules.rml.ttl -o graph/{os.path.splitext(mapper)[0]}-{index}-graph.ttl -s turtle')


if __name__ == '__main__':
    # define what mapping templates will be used for chunks
    mapping = {
        'store-a-mapper.yml': [('2020-06-16', 'Store_A.csv~csv'), ('2020-06-17', 'Store_A.csv~csv')],
        # 'store-b-mapper.yml': [('2020-06-16', 'Store_B.json'), ('2020-06-17', 'Store_B.json')],
        'store-c-mapper.yml': [('2020-06-16', 'Store_C.csv~csv'), ('2020-06-17', 'Store_C.csv~csv')]
    }

    # flat the structure
    mapping = flattern_mappings()

    print("Processing started ... ")

    pool = multiprocessing.Pool()
    pool.map(job, mapping)
    pool.close()
    pool.join()

    print("Post process cleaning ... ")

    os.system("rm -rf resources/tmp/*")
    os.system("cat graph/store-*-graph.ttl > graph/graph.ttl")
    os.system("rm -rf graph/store-*-*.ttl")
