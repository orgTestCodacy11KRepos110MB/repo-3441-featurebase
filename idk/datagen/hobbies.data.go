package datagen

var hobbies = []string{
	"Lego building", "Watching movies", "Watch making", "Slacklining", "BMX", "Cricket",
	"Sketching", "Satellite watching", "Volunteering", "Radio-controlled model playing",
	"Stone collecting", "Picnicking", "Hydroponics", "Karate", "Roller skating", "Skateboarding",
	"Element collecting", "Weaving", "Beach volleyball", "Archery", "Livestreaming", "Stone skipping",
	"Trapshooting", "Filmmaking", "Diorama", "Makeup", "Rugby league football", "Community activism",
	"Field hockey", "Backpacking", "Slot car", "Insect collecting", "VR Gaming", "Video making",
	"Bowling", "Sled dog racing", "Skiing", "Web design", "Sand art", "Public speaking",
	"Movie memorabilia collecting", "Gardening", "Wikipedia editing", "Croquet", "Mathematics",
	"Rail transport modeling", "Darts", "Judo", "Equestrianism", "Figure Skating", "Scrapbooking",
	"Airbrushing", "Photography", "Climbing", "Tourism", "Journaling", "Flower growing",
	"Wood carving", "Fashion design", "Polo", "Slot car racing", "Reading", "Electronic games",
	"Martial arts", "Bell ringing", " Air sports", "Skipping rope", "Bowling", "Caving",
	"Leather crafting", "Construction", "Bus riding", "Flag football", "Anime", "Whittling",
	"Aerospace", "Sun bathing", "Music", "Running", "Diving", "Plastic art", "Stamp collecting",
	"Gymnastics", "Kabaddi", "Coin collecting", "Video editing", "Stripping", "Cribbage",
	"Candy making", "Amateur geology", "Motor sports", "Sculpting", "Transit map collecting",
	"Refinishing", "Surfing", "Swimming", "Skateboarding", "Knowledge/word games", "Tether car",
	"Poi", "Manga", " Action figure", "Teaching", "Blacksmithing", "Fingerpainting", "Audiophile",
	"Spreadsheets", "Scouting", "Frisbee", "Metal detecting", "Book collecting", "Radio-controlled model playing",
	"Films", "Karaoke", "Wargaming", "Biology", "DJing", "Axe throwing", "Volleyball",
	"Life Science", "Fossil hunting", "Beachcombing", "Sudoku", "Cross-stitch", "Ephemera collecting",
	"Puzzles", "Hiking/backpacking", "Digital hoarding", "Horseshoes", "Amateur astronomy",
	"Book discussion clubs", "Model building", "Ceramics", "Telling jokes", "Gardening",
	"Renaissance fair", "Record collecting", "Collecting", "Taxidermy", "Flying", "Zumba",
	" Archaeology", "Quidditch", "Playing musical instruments", "Tapestry", "Perfume",
	"Philately", "Business", "Microbiology", "Rafting", "Postcrossing", "Whisky", "Botany",
	"Badminton", "Chatting", "Board sports", "Groundhopping", "Inventing", "Paragliding",
	"Shooting sport", "Esports", "Sport stacking", "Proverbs", "Marching band", "Feng shui decorating",
	"Car tuning", "Sociology", "Writing music", "Robot combat", "Parkour", "Shogi", "Weightlifting",
	"Fashion", "Safari", "Motorcycling", "Pool", "Meteorology", "Auto audiophilia", "Mushroom hunting/mycology",
	"Radio-controlled model playing", "Miniature art", "Video game developing", "Medical science",
	"Herp keeping", "Shoemaking", "Gongfu tea", "Dowsing", "Microscopy", "Welding", "Woodworking",
	"Clothesmaking", "Fingerprint collecting", "Crossword puzzles", "Breadmaking", "Ice hockey",
	"Dolls", "Curling", "Sailing", "Mazes (indoor/outdoor)", "Fishkeeping", "Ticket collecting",
	"Flower arranging", "Nail art", "Couponing", "Skimboarding", "Fishing", "Figure skating",
	"Herping", "Surfing", "Go", "Vintage clothing", "Shortwave listening", "Water sports",
	"Darts", "Bonsai", "Lomography", "Crocheting", "Meditation", "Cornhole", "Railway journeys",
	"Cardistry", "Book restoration", "Graffiti", "Decorating", "Yo-yoing", "Speedcubing",
	"Lotology (lottery ticket collecting)", "Houseplant care", "Cryptography", "Quilling",
	"Powerlifting", "Cheesemaking", "Table tennis", "Public transport riding", "Pet adoption & fostering",
	"Magnet fishing", "Hooping", "Bridge", "Rubik's Cube", "Beekeeping", "Digital arts",
	"Foreign language learning", "Race walking", "Fusilately (phonecard collecting)",
	"Fishfarming", "Jigsaw puzzles", "Reviewing Gadgets", "Entrepreneurship", "Pickleball",
	"Wine tasting", "Footbag", "Astronomy", "Stuffed toy collecting", "Roller derby",
	"Astrology", "Furniture building", "Lapidary", "Iceboat racing", "High-power rocketry",
	"Reiki", "Baking", "Automobilism", "Witchcraft", "Walking", "Aerial silk", "Gongoozling",
	"Learning", "Cartophily (card collecting)", "Paintball", "Genealogy", "Do it yourself",
	"Volleyball", "Science and technology studies", "Horsemanship", "Swimming", "Needlepoint",
	"Fishkeeping", "Vintage cars", "Basketball", "Qigong", "Video game collecting", "Writing",
	"Vacation", "Nordic skating", "Powerboat racing", "Baseball", "Candle making", "Whale watching",
	"Knot tying", "Ice skating", "Debate", "Checkers (draughts)", "Board/tabletop games",
	"Model engineering", "VR Gaming", "Palmistry", "Air hockey", "Pole dancing", "Modeling",
	"Puppetry", "Memory training", "Sculling or rowing", "Seashell collecting", "Poetry",
	"Role-playing games", "Flying model planes", "Tennis polo", "Gymnastics", "Metalworking",
	"Scutelliphily", "Eating", "Pet sitting", "Fruit picking", "Farming", "Survivalism",
	"Fly tying", "Wax sealing", "Sea glass collecting", "Antiquing", "Metal detecting",
	"Guerrilla gardening", "Dance", "Birdwatching", "Skiing", "Jujitsu", "Hiking", "Model aircraft",
	"Model United Nations", "Jukskei", "Leaves", "Drama", "Lacrosse", "LARPing", "Home improvement",
	"Skydiving", "Snowmobiling", "Meteorology", "Fantasy sports", "Blogging", "Hobby horsing",
	"Knife throwing", "English", "Soapmaking", "Talking", "Lace making", "Driving", "Engraving",
	"Kung fu", "Laser tag", "Composting", "Sledding", "Croquet", "Railway studies", "Magic",
	"Kite flying", "Acting", "Juggling", "Travel", "Glassblowing", "Baton twirling", "Boxing",
	"Kart racing", "Comic book collecting", "Meditation", "Mineral collecting", "Dancing",
	"Antiquities", "Ultimate frisbee", "Planning", "Pole dancing", "Snorkeling", "Zoo visiting",
	"Animation", "Rock painting", "Exhibition drill", "Stamp collecting", "People-watching",
	"Knife collecting", "Herbalism", "Knitting", "Karting", "Tennis", "Drink mixing",
	"Kombucha brewing", "Chemistry", "Badminton", "Lock picking", "Letterboxing", "Storm chasing",
	"Sports memorabilia", "Tai chi", "Calligraphy", "Weight training", "Pin (lapel)",
	"Coffee roasting", "Unicycling", "Ghost hunting", "Archery", "Museum visiting", "Card games",
	"Dog sport", "Herping", "Netball", "Video gaming", "Trade fair visiting", "Baseball",
	"plush collecting", "Car fixing & building", "Tatebanko", "BASE jumping", "Gold prospecting",
	"Animal fancy", "Jogging", "Gunsmithing", "Shooting", "Long-distance running", "Quizzes",
	"Canoeing", "Aquascaping", "Practical jokes", "Tattooing", "Social studies", "Vehicle restoration",
	"Cheerleading", "Proofreading and editing", "Fishing", "Squash", "Tarot", "Sewing",
	"Birdwatching", "Cycling", "Button collecting", "Animation", "Art", "Giving advice",
	"Handball", "Die-cast toy", "Jewelry making", "Deltiology (postcard collecting)",
	"Brazilian jiu-jitsu", "Coloring", "Podcast hosting", "Couch surfing", "Reading",
	"Compact discs", "Bullet journaling", "Hunting", "Australian rules football", "Origami",
	"Tea bag collecting", "Webtooning", "Longboarding", "Auto detailing", "Hacking", "Kendama",
	"Photography", "Pilates", "Snowboarding", "Pressed flower craft", "Conlanging", "Beatboxing",
	"Amateur radio", "Freestyle football", "Mountaineering", "Rock tumbling", "Yoga",
	"Bus spotting", "Tour skating", "Rock balancing", "Camping", "Sculling or rowing",
	"Performance", "Djembe", "Entertaining", "Chess", "Cleaning", "Electronics", "Vinyl Records",
	"Beauty pageants", "Auto racing", "Climbing", "Road biking", "Gingerbread house making",
	"Distro Hopping", "Geocaching", "Snowshoeing", "Creative writing", "Taekwondo", "Radio-controlled car racing",
	"Worldbuilding", "Car riding", "Stand-up comedy", "Flying disc", "Dog walking", "Phillumeny",
	"Foraging", "Singing", "Barbershop Music", "Confectionery", "Amusement park visiting",
	"Inline skating", "Knife making", "History", "Breakdancing", "Experimenting", "Color guard",
	"Painting", "Soccer", "Backgammon", "City trip", "Marbles", "Renovating", "Speed skating",
	"Handball", "Gaming", "Triathlon", "Mountain biking", "Machining", "Art collecting",
	"Baton twirling", "Horseback riding", "Benchmarking", "Philately", "Tourism", "Wrestling",
	"Disc golf", "Flower collecting and pressing", "Fitness", "Acroyoga", "Beer tasting",
	"Video gaming", "Lacrosse", "Bodybuilding", "Thrifting", "Topiary", "3D printing",
	"Crystals", "Orienteering", "Noodling", "Geocaching", "Orienteering", "Winemaking",
	"Watching documentaries", "Pet", "Drawing", "Photography", "Airsoft", "Homebrewing",
	"Aircraft spotting", "Mini Golf", "Storytelling", "Pickleball", "Shuffleboard", "Cooking",
	"Rock climbing", "Vegetable farming", "Radio-controlled model playing", "Billiards",
	"Association football", "Embroidery", "Waxing", "Physics", "Hobby tunneling", "Scuba diving",
	"Kayaking", "Videography", "Tennis", "Slot car", "Table tennis", "Golfing", "Dog training",
	"Craft", "Mahjong", "Cycling", "Thru-hiking", "Fencing", "Airsoft", "Humor", "Mycology",
	"Rail transport modelling", "Sports science", "Table football", "Trainspotting", "Minimalism",
	"Urban exploration", "Macrame", "Computer programming", "Horseback riding", "Cue sports",
	"Magic", "Pyrography", "Ice skating", "Upcycling", "Shoes", "Power Nap", "Pen Spinning",
	"Jumping rope", "Astronomy", "Pottery", "Martial arts", "Butterfly watching", "Hula hooping",
	"Water polo", "Geography", "Chess", "Rugby", "Cosplaying", "Racquetball", "Shopping",
	"Graphic design", "Binge-watching", "Kitesurfing", "Research", "Model racing", "Listening to podcasts",
	"Radio-controlled model collecting", "Research", "Rapping", "Poker", "Rappelling",
	"Watching television", "Listening to music", "Mechanics", "Philosophy", "Recipe creation",
	"Quilting", "Fossicking", "Social media", "Word searches", "Massaging", "Dominoes",
	"Longboarding", "Scuba Diving", "Dining", "Hardware", "Communication", "Ant-keeping",
	"Canyoning", "Dandyism", "Psychology", "Softball", "Table tennis playing"}
