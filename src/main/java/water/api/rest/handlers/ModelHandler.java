package water.api.rest.handlers;

import water.DKV;
import water.Iced;
import water.Key;
import water.api.rest.REST.Versioned;
import water.api.rest.Version;

abstract public class ModelHandler<V extends Version> extends DKVHandler<V> {
    public ModelHandler(String path) {
        super(path);
    }
}
