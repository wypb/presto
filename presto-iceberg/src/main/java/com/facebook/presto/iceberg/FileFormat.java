/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg;

import com.google.common.base.Preconditions;
import org.apache.iceberg.types.Comparators;

import java.util.Locale;

public enum FileFormat
{
    ORC("orc", true),
    PARQUET("parquet", true),
    AVRO("avro", true),
    METADATA("metadata.json", false);

    private final String ext;
    private final boolean splittable;

    FileFormat(String ext, boolean splittable)
    {
        this.ext = "." + ext;
        this.splittable = splittable;
    }

    public boolean isSplittable()
    {
        return splittable;
    }

    /**
     * Returns filename with this format's extension added, if necessary.
     *
     * @param filename a filename or path
     * @return if the ext is present, the filename, otherwise the filename with ext added
     */
    public String addExtension(String filename)
    {
        if (filename.endsWith(ext)) {
            return filename;
        }
        return filename + ext;
    }

    public static FileFormat fromFileName(CharSequence filename)
    {
        for (FileFormat format : FileFormat.values()) {
            int extStart = filename.length() - format.ext.length();
            if (Comparators.charSequences()
                    .compare(format.ext, filename.subSequence(extStart, filename.length()))
                    == 0) {
                return format;
            }
        }

        return null;
    }

    public static FileFormat fromString(String fileFormat)
    {
        Preconditions.checkArgument(null != fileFormat, "Invalid file format: null");
        try {
            return FileFormat.valueOf(fileFormat.toUpperCase(Locale.ENGLISH));
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Invalid file format: %s", fileFormat), e);
        }
    }
}
