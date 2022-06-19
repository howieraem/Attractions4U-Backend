import numpy as np
import os
from PIL import Image
import torch
import torch.nn as nn
import torchvision.models as models
import torchvision.transforms as transforms
from tqdm import trange


arch = 'resnet50'
save_pt_path = '%s_places365.pt' % arch
img_path = "12.jpg"
device = torch.device("cuda:0") if torch.cuda.is_available() else ("cpu")
model = models.__dict__[arch](num_classes=365, pretrained=False)
model.load_state_dict(torch.load(save_pt_path))

# We only need the feature vectors
model.fc = nn.Identity()
model.classifier = nn.Identity()

model.to(device)
model.eval()

normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                 std=[0.229, 0.224, 0.225])
my_transforms = transforms.Compose([
    transforms.Resize(256),
    transforms.ToTensor(),
    normalize,
])


def get_vec(img_path):
    img = Image.open(img_path).convert('RGB')
    tensor = my_transforms(img).to(device).unsqueeze(0)
    with torch.no_grad():
        outputs = model.forward(tensor)
    # outputs = nn.functional.normalize(outputs, dim=1)
    return outputs.squeeze(0).cpu().numpy()


def main():
    img_paths = sorted(os.listdir('imgs'))
    vecs = []
    for i in trange(len(img_paths)):
        img_path = os.path.join('imgs', img_paths[i])
        vecs.append(get_vec(img_path))
    vecs = np.stack(vecs, axis=0)
    print(vecs.shape)
    np.save(f'{arch}_vecs.npy', vecs)


if __name__ == '__main__':
    main()
