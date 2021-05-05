package com.datafibers.cdf;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class YamlConfigReader {
    private static Constructor constructor = new MyConstructor();

    private static Pattern systemPropertyMatcher = Pattern.compile("\\$\\{([^}^{]+)\\}}");

    private static class ImportConstruct extends AbstractConstruct {
        @Override
        public Object construct(Node node) {
            if(!(node instanceof ScalarNode))
                throw new IllegalArgumentException("Non-scalar !import: " + node.toString());
            ScalarNode scalarNode = (ScalarNode) node;
            String value = scalarNode.getValue();
            Matcher matcher = systemPropertyMatcher.matcher(value);
            while (matcher.find()) {
                String group = matcher.group();
                String sysPropName = group.substring(2, group.length() - 1);
                String sysProp = System.getProperty(sysPropName);
                if(sysProp == null)
                    throw new IllegalArgumentException("Could not find System Property: " + sysPropName);
                value = value.replace(group, sysProp);
            }
            return value;
        }
    }

    private static class MyConstructor extends Constructor {
        MyConstructor() {
            yamlConstructors.put(new Tag("!path"), new ImportConstruct());
        }
    }

    public static class CustomResolver extends Resolver {
        /* this method is used to parse scalars as string */
        @Override
        protected void addImplicitResolvers() {
            addImplicitResolver(new Tag("!path"), systemPropertyMatcher, null);
        }
    }

    public static Map readYaml(String path) throws FileNotFoundException {
        File f = new File(path);
        InputStream input = new FileInputStream(f);
        Yaml yaml = new Yaml(constructor, new Representer(), new DumperOptions(), new CustomResolver());
        Object object = yaml.load(input);
        return (Map) object;
    }

    public static Map readYamlFromResources(InputStream input) {
        Yaml yaml = new Yaml(constructor, new Representer(), new DumperOptions(), new CustomResolver());
        Object object = yaml.load(input);
        System.out.println();
        return (Map) object;
    }
}
