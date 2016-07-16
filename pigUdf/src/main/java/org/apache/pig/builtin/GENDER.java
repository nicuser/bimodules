/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.pig.builtin;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 *
 * @author lsamud001c
 */
public class GENDER extends EvalFunc<Tuple> {

    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    public Tuple exec(Tuple input) throws IOException {
        String gender = "Unknown";

        try {
            String genderMatcher = (String) input.get(1);

            if (genderMatcher.length() == 6) {
                String genderPattern = genderMatcher.substring(2, 4);
                System.out.println("genderPattern:"+genderPattern);
                if (Integer.parseInt(genderPattern) > 12) {
                    gender = "Female";
                } else {
                    gender = "Male";
                }
            }
            Tuple result = mTupleFactory.newTuple(3);
            result.set(0, input.get(0));
            result.set(1, gender);
            result.set(2, input.get(2));

            return result;
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        return null;
    }
}
