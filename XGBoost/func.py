def chunk(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

def idx_to_onehot(idx, num_elements):
    #onehot = np.zeros(num_elements, dtype=np.float32)
    onehot=[0]*num_elements
    onehot[idx] = 1.
    return onehot

def set_encode(source_set, onehot=True):
    num_elements = len(source_set)
    source_list = list(source_set)
    # Sort list to avoid non-deterministic behavior
    source_list.sort()
    # Build map from s to i
    thing2idx = {s: i for i, s in enumerate(source_list)}
    # Build array (essentially a map from idx to s)
    idx2thing = [s for i, s in enumerate(source_list)]
    if onehot:
        thing2vec = {s: idx_to_onehot(i, num_elements) for i, s in enumerate(source_list)}
        return thing2vec, idx2thing
    return thing2idx, idx2thing