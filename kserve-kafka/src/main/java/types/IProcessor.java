package types;

import app.KfServingRequester;

public interface IProcessor<I,O> {

    public KfServingRequester<I, O> getRequester();
    public O process(I input);

}
