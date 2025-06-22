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
public class AppendEntry {
    private byte[] data;
    private boolean isEmpty;

    public static AppendEntry unmarshal(byte[] bytes) {
        return JsonUtils.fromJson(bytes, AppendEntry.class);
    }

    public byte[] marshal() {
        return JsonUtils.jsonBytes(this);
    }
}
