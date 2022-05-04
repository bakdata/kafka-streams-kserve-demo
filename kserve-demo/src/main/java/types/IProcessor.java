package types;

import app.KServeRequester;

public interface IProcessor<I,O> {

    public KServeRequester<I, O> getRequester();
    public O process(I input);

}
