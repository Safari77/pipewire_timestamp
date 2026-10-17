/* Optparse --- portable, reentrant, embeddable, getopt-like option parser
 *
 * This is free and unencumbered software released into the public domain.
 *
 * To get the implementation, define OPTPARSE_IMPLEMENTATION.
 * Optionally define OPTPARSE_API to control the API's visibility
 * and/or linkage (static, __attribute__, __declspec).
 *
 * The POSIX getopt() option parser has three fatal flaws. These flaws
 * are solved by Optparse.
 *
 * 1) Parser state is stored entirely in global variables, some of
 * which are static and inaccessible. This means only one thread can
 * use getopt(). It also means it's not possible to recursively parse
 * nested sub-arguments while in the middle of argument parsing.
 * Optparse fixes this by storing all state on a local struct.
 *
 * 2) The POSIX standard provides no way to properly reset the parser.
 * This means for portable code that getopt() is only good for one
 * run, over one argv with one option string. It also means subcommand
 * options cannot be processed with getopt(). Most implementations
 * provide a method to reset the parser, but it's not portable.
 * Optparse provides an optparse_arg() function for stepping over
 * subcommands and continuing parsing of options with another option
 * string. The Optparse struct itself can be passed around to
 * subcommand handlers for additional subcommand option parsing. A
 * full reset can be achieved by with an additional optparse_init().
 *
 * 3) Error messages are printed to stderr. This can be disabled with
 * opterr, but the messages themselves are still inaccessible.
 * Optparse solves this by writing an error message in its errmsg
 * field. The downside to Optparse is that this error message will
 * always be in English rather than the current locale.
 *
 * Optparse should be familiar with anyone accustomed to getopt(), and
 * it could be a nearly drop-in replacement. The option string is the
 * same and the fields have the same names as the getopt() global
 * variables (optarg, optind, optopt).
 *
 * Optparse also supports GNU-style long options with optparse_long().
 * The interface is slightly different and simpler than getopt_long().
 *
 * If the "positional" pointer is non-null, positional arguments are
 * accumulated in this array, and so options and positional arguments
 * may be mixed, i.e. they will be permuted. The original argv is left
 * unmodified.
 */
#ifndef OPTPARSE_H
#define OPTPARSE_H

#ifndef OPTPARSE_API
#define OPTPARSE_API
#endif

struct optparse {
    char** argv;
    char** positional;
    int npositional;
    int optind;
    int optopt;
    char* optarg;
    char errmsg[64];
    int subopt;
};

enum optparse_argtype { OPTPARSE_NONE, OPTPARSE_REQUIRED, OPTPARSE_OPTIONAL };

struct optparse_long {
    const char* longname;
    int shortname;
    enum optparse_argtype argtype;
};

/**
 * Initializes the parser state.
 * @param positional NULL, or an array with room for argc entries
 */
OPTPARSE_API
struct optparse optparse_init(char** argv, char** positional);

/**
 * Read the next option in the argv array.
 * @param optstring a getopt()-formatted option string.
 * @return the next option character, -1 for done, or '?' for error
 *
 * Just like getopt(), a character followed by no colons means no
 * argument. One colon means the option has a required argument. Two
 * colons means the option takes an optional argument.
 */
OPTPARSE_API
int optparse(struct optparse* options, const char* optstring);

/**
 * Handles GNU-style long options in addition to getopt() options.
 * This works a lot like GNU's getopt_long(). The last option in
 * longopts must be all zeros, marking the end of the array. The
 * longindex argument may be NULL.
 */
OPTPARSE_API
int optparse_long(struct optparse* options,
                  const struct optparse_long* longopts, int* longindex);

/**
 * Used for stepping over non-option arguments.
 * @return the next non-option argument, or NULL for no more arguments
 *
 * Argument parsing can continue with optparse() after using this
 * function. That would be used to parse the options for the
 * subcommand returned by optparse_arg(). This function allows you to
 * ignore the value of optind.
 */
OPTPARSE_API
char* optparse_arg(struct optparse* options);

/* Implementation */
#ifdef OPTPARSE_IMPLEMENTATION

/* 96 ASCII printable characters, each with up to two colons. */
#define OPTPARSE_OPTSTRING_MAX (96 * 3 + 1)

#define OPTPARSE_MSG_INVALID "invalid option"
#define OPTPARSE_MSG_MISSING "option requires an argument"
#define OPTPARSE_MSG_TOOMANY "option takes no arguments"

static int optparse_error(struct optparse* options, const char* msg,
                          const char* data) {
    unsigned p = 0;
    const char* sep = " -- '";
    unsigned max = (unsigned)sizeof(options->errmsg) - 1; /* room for '\0' */
    while (*msg && p < max) options->errmsg[p++] = *msg++;
    while (*sep && p < max) options->errmsg[p++] = *sep++;
    while (*data && p < max - 1) /* room for the closing quote */
        options->errmsg[p++] = *data++;
    if (p < max) {
        options->errmsg[p++] = '\'';
    }
    options->errmsg[p] = '\0';
    return '?';
}

OPTPARSE_API
struct optparse optparse_init(char** argv, char** positional) {
    struct optparse options;
    options.argv = argv;
    options.positional = positional;
    options.npositional = 0;
    options.optind = argv[0] != 0;
    options.optopt = 0;
    options.optarg = 0;
    options.errmsg[0] = '\0';
    options.subopt = 0;
    return options;
}

static int optparse_is_dashdash(const char* arg) {
    return arg != 0 && arg[0] == '-' && arg[1] == '-' && arg[2] == '\0';
}

static int optparse_is_shortopt(const char* arg) {
    return arg != 0 && arg[0] == '-' && arg[1] != '-' && arg[1] != '\0';
}

static int optparse_is_longopt(const char* arg) {
    return arg != 0 && arg[0] == '-' && arg[1] == '-' && arg[2] != '\0';
}

static int optparse_argtype(const char* optstring, char c) {
    int count = OPTPARSE_NONE;
    if (c == ':') return -1;
    for (; *optstring && c != *optstring; optstring++);
    if (!*optstring) return -1;
    if (optstring[1] == ':') count += optstring[2] == ':' ? 2 : 1;
    return count;
}

static void optparse_copy_positional(struct optparse* options) {
    if (options->positional) {
        while (options->argv[options->optind]) {
            options->positional[options->npositional++] =
                options->argv[options->optind++];
        }
    }
}

OPTPARSE_API
int optparse(struct optparse* options, const char* optstring) {
    int type;
    char* next;
    char* option = 0;
    for (;;) {
        option = options->argv[options->optind];
        options->errmsg[0] = '\0';
        options->optopt = 0;
        options->optarg = 0;
        if (option == 0) {
            return -1;
        } else if (optparse_is_dashdash(option)) {
            options->optind++; /* consume "--" */
            optparse_copy_positional(options);
            return -1;
        } else if (!optparse_is_shortopt(option)) {
            if (options->positional) {
                options->optind++; /* consume */
                options->positional[options->npositional++] = option;
            } else {
                return -1;
            }
        } else {
            break;
        }
    }

    option += options->subopt + 1;
    options->optopt = option[0];
    type = optparse_argtype(optstring, option[0]);
    next = options->argv[options->optind + 1];
    switch (type) {
        case -1: {
            char str[2] = {0, 0};
            str[0] = option[0];
            if (option[1]) {
                options->subopt++;
            } else {
                options->subopt = 0;
                options->optind++;
            }
            return optparse_error(options, OPTPARSE_MSG_INVALID, str);
        }
        case OPTPARSE_NONE:
            if (option[1]) {
                options->subopt++;
            } else {
                options->subopt = 0;
                options->optind++;
            }
            return option[0];
        case OPTPARSE_REQUIRED:
            options->subopt = 0;
            options->optind++;
            if (option[1]) {
                options->optarg = option + 1;
            } else if (next != 0) {
                options->optarg = next;
                options->optind++;
            } else {
                char str[2] = {0, 0};
                str[0] = option[0];
                options->optarg = 0;
                return optparse_error(options, OPTPARSE_MSG_MISSING, str);
            }
            return option[0];
        case OPTPARSE_OPTIONAL:
            options->subopt = 0;
            options->optind++;
            if (option[1])
                options->optarg = option + 1;
            else
                options->optarg = 0;
            return option[0];
    }
    return 0;
}

OPTPARSE_API
char* optparse_arg(struct optparse* options) {
    char* option = options->argv[options->optind];
    options->subopt = 0;
    if (option != 0) options->optind++;
    return option;
}

static int optparse_longopts_end(const struct optparse_long* longopts, int i) {
    return !longopts[i].longname && !longopts[i].shortname;
}

static void optparse_from_long(const struct optparse_long* longopts,
                               char* optstring, int size) {
    char* p = optstring;
    char* end = optstring + size - 1; /* keep room for the '\0' */
    int i;
    for (i = 0; !optparse_longopts_end(longopts, i); i++) {
        int shortname = longopts[i].shortname;
        int argtype = (int)longopts[i].argtype;
        int a;
        /* ':' would be read back as an argument marker */
        if (shortname > 0 && shortname < 127 && shortname != ':') {
            if (argtype < OPTPARSE_NONE) argtype = OPTPARSE_NONE;
            if (argtype > OPTPARSE_OPTIONAL) argtype = OPTPARSE_OPTIONAL;
            /* check remaining capacity */
            if (end - p < argtype + 1) break;

            *p++ = (char)shortname;
            for (a = 0; a < argtype; a++) *p++ = ':';
        }
    }
    *p = '\0';
}

/* Unlike strcmp(), handles options containing "=". */
static int optparse_longopts_match(const char* longname, const char* option) {
    const char *a = option, *n = longname;
    if (longname == 0) return 0;
    for (; *a && *n && *a != '='; a++, n++)
        if (*a != *n) return 0;
    return *n == '\0' && (*a == '\0' || *a == '=');
}

/* Return the part after "=", or NULL. */
static char* optparse_longopts_arg(char* option) {
    for (; *option && *option != '='; option++);
    if (*option == '=')
        return option + 1;
    else
        return 0;
}

static int optparse_long_fallback(struct optparse* options,
                                  const struct optparse_long* longopts,
                                  int* longindex) {
    int result;
    char optstring[OPTPARSE_OPTSTRING_MAX];
    optparse_from_long(longopts, optstring, (int)sizeof(optstring));
    result = optparse(options, optstring);
    if (longindex != 0) {
        *longindex = -1;
        if (result != -1) {
            int i;
            for (i = 0; !optparse_longopts_end(longopts, i); i++)
                if (longopts[i].shortname == options->optopt) *longindex = i;
        }
    }
    return result;
}

OPTPARSE_API
int optparse_long(struct optparse* options,
                  const struct optparse_long* longopts, int* longindex) {
    int i;
    char* option = 0;

    for (;;) {
        option = options->argv[options->optind];
        options->errmsg[0] = '\0';
        options->optopt = 0;
        options->optarg = 0;
        if (option == 0) {
            return -1;
        } else if (optparse_is_dashdash(option)) {
            options->optind++; /* consume "--" */
            optparse_copy_positional(options);
            return -1;
        } else if (optparse_is_shortopt(option)) {
            return optparse_long_fallback(options, longopts, longindex);
        } else if (!optparse_is_longopt(option)) {
            if (options->positional) {
                options->optind++; /* consume */
                options->positional[options->npositional++] = option;
            } else {
                return -1;
            }
        } else {
            break;
        }
    }

    /* Parse as long option. */
    option += 2; /* skip "--" */
    options->optind++;
    for (i = 0; !optparse_longopts_end(longopts, i); i++) {
        const char* name = longopts[i].longname;
        if (optparse_longopts_match(name, option)) {
            char* arg;
            if (longindex) *longindex = i;
            options->optopt = longopts[i].shortname;
            arg = optparse_longopts_arg(option);
            if (longopts[i].argtype == OPTPARSE_NONE && arg != 0) {
                return optparse_error(options, OPTPARSE_MSG_TOOMANY, name);
            } else if (arg != 0) {
                options->optarg = arg;
            } else if (longopts[i].argtype == OPTPARSE_REQUIRED) {
                options->optarg = options->argv[options->optind];
                if (options->optarg == 0)
                    return optparse_error(options, OPTPARSE_MSG_MISSING, name);
                else
                    options->optind++;
            }
            return options->optopt;
        }
    }
    return optparse_error(options, OPTPARSE_MSG_INVALID, option);
}

#endif /* OPTPARSE_IMPLEMENTATION */
#endif /* OPTPARSE_H */
