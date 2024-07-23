package dev.restate.examples.azure;

import dev.restate.sdk.JsonSerdes;
import dev.restate.sdk.ObjectContext;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.VirtualObject;
import dev.restate.sdk.common.StateKey;

@VirtualObject
public class GreeterObject {

    private static final StateKey<Integer> COUNT =
            StateKey.of("available-drivers", JsonSerdes.INT);

    @Handler
    public String greet(ObjectContext ctx, String greeting) {

        // Access the state attached to this object (this 'name')
        // State access and updates are exclusive and consistent with the invocations
        int count = ctx.get(COUNT).orElse(0);
        int newCount = count + 1;
        ctx.set(COUNT, newCount);

        return String.format("%s %s, for the %d-th time", greeting, ctx.key(), newCount);
    }

    @Handler
    public String ungreet(ObjectContext ctx) {
        int count = ctx.get(COUNT).orElse(0);
        if (count > 0) {
            int newCount = count - 1;
            ctx.set(COUNT, newCount);
        }

        return "Dear " + ctx.key() + ", taking one greeting back: " + count;
    }

}
