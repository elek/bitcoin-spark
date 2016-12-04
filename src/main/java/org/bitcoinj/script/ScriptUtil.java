package org.bitcoinj.script;

import org.bitcoinj.core.ScriptException;
import org.bitcoinj.core.Utils;

import java.util.Optional;

/**
 * Created by melek on 9/27/16.
 */
public class ScriptUtil {
    private static final String STANDARD = "DUP[1] HASH160[1] EQUALVERIFY[1] CHECKSIG";

    public static Optional<String> toAddress(byte[] raw) {
        try {
            Script script = new Script(raw);
            return Optional.of(Utils.byteArrayToAddress((byte) 0, script.getPubKeyHash()));
        } catch (ScriptException ex) {
            return Optional.empty();
        }
    }

    public static String parseToString(byte[] raw) {
        try {
            String prevScript = null;
            int counter = 1;
            StringBuilder b = new StringBuilder();
            Script script = new Script(raw);
            for (ScriptChunk chunk : script.getChunks()) {

                if (chunk.isOpCode()) {
                    String currentScript = chunk.toString();
                    if (prevScript != null && !prevScript.equals(currentScript)) {
                        b.append("[" + counter + "]");
                        counter = 1;
                        b.append(" " + chunk);
                    }
                    if (prevScript != null && prevScript.equals(currentScript)) {
                        counter++;
                    }
                    if (prevScript == null) {
                        b.append(chunk);
                    }
                    prevScript = currentScript;
                }
            }
            return b.toString().trim();
        } catch (Exception ex) {
            return "ERROR";
        }
    }
}
