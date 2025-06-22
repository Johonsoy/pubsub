package org.example.core;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.example.utils.JsonUtils;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class PubData {
    private byte[] data;
    private long timestamp;
    private String address;
    private String topic;
    public byte[] marshal() {
        return JsonUtils.jsonBytes(this);
    }

    public static PubData unmarshal(byte[] data) {
        return JsonUtils.fromJson(data, PubData.class);
    }


}
