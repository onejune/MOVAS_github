import torch
import torch.nn as nn
import argparse, math, numpy as np
from load_data import get_data
from models import CTranModel
from models import CTranModelCub
from config_args import get_args
import torch.nn.functional as F
import utils.evaluate as evaluate
import utils.logger as logger
from pdb import set_trace as stop
from optim_schedule import WarmupLinearSchedule
from run_epoch import run_epoch

args = get_args(argparse.ArgumentParser())

print("Labels: {}".format(args.num_labels))
print("Train Known: {}".format(args.train_known_labels))
print("Test Known:  {}".format(args.test_known_labels))

train_loader, valid_loader, test_loader = get_data(args)

if args.dataset == "cub":
    model = CTranModelCub(
        args.num_labels,
        args.use_lmt,
        args.pos_emb,
        args.layers,
        args.heads,
        args.dropout,
        args.no_x_features,
    )
    print(model.self_attn_layers)
else:
    model = CTranModel(
        args.num_labels,
        args.use_lmt,
        args.pos_emb,
        args.layers,
        args.heads,
        args.dropout,
        args.no_x_features,
    )
    print(model.self_attn_layers)


def load_saved_model(saved_model_name, model):
    checkpoint = torch.load(saved_model_name)
    model.load_state_dict(checkpoint["state_dict"])
    return model


print('model_name:', args.model_name)

if torch.cuda.device_count() > 1:
    print("Using", torch.cuda.device_count(), "GPUs!")
    model = nn.DataParallel(model)
model = model.cuda()

if args.inference:
    print('saved_model_name:', args.saved_model_name)
    model = load_saved_model(args.saved_model_name, model)
    if test_loader is not None:
        data_loader = test_loader
    else:
        data_loader = valid_loader

    all_preds, all_targs, all_masks, all_ids, test_loss, test_loss_unk = run_epoch(
        args, model, data_loader, None, 1, "Testing")
    test_metrics = evaluate.compute_metrics(
        args,
        all_preds,
        all_targs,
        all_masks,
        test_loss,
        test_loss_unk,
        0,
        args.test_known_labels,
    )

    print('all_preds.shape:', all_preds.size())
    optimal_threshold = 0.1
    all_preds = F.sigmoid(all_preds)
    all_predictions = all_preds.numpy()
    top_3rd = np.sort(all_predictions)[:, -3].reshape(-1, 1)
    all_predictions_top3 = all_predictions.copy()
    all_predictions_top3[all_predictions_top3 < top_3rd] = 0
    all_predictions_top3[all_predictions_top3 < optimal_threshold] = 0
    all_predictions_top3[all_predictions_top3 >= optimal_threshold] = 1

    all_targs = all_targs.numpy()
 				
    fout = open(args.predict_out_path, 'w')
    real_label_names = data_loader.dataset.label_names
    print('all label_names:', real_label_names)
    for k in range(len(all_ids)):
        p_labels = all_predictions_top3[k].tolist()
        r_labels = all_targs[k].tolist()
        p_labels = [real_label_names[i] for i, v in enumerate(p_labels) if v > 0]
        r_labels = [real_label_names[i] for i, v in enumerate(r_labels) if v > 0]
        fout.write(all_ids[k] + '\t' + 'p_labels: ' + ','.join(p_labels) +
                   '\t' + 'r_labels: ' + ','.join(r_labels) + '\n')

    exit(0)

if args.freeze_backbone:
    for p in model.module.backbone.parameters():
        p.requires_grad = False
    for p in model.module.backbone.base_network.layer4.parameters():
        p.requires_grad = True

if args.optim == "adam":
    optimizer = torch.optim.Adam(filter(lambda p: p.requires_grad,
                                        model.parameters()),
                                 lr=args.lr)  # , weight_decay=0.0004)
else:
    optimizer = torch.optim.SGD(
        filter(lambda p: p.requires_grad, model.parameters()),
        lr=args.lr,
        momentum=0.9,
        weight_decay=1e-4,
    )

if args.warmup_scheduler:
    step_scheduler = None
    scheduler_warmup = WarmupLinearSchedule(optimizer, 1, 300000)
else:
    scheduler_warmup = None
    if args.scheduler_type == "plateau":
        step_scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer,
                                                                    mode="min",
                                                                    factor=0.1,
                                                                    patience=5)
    elif args.scheduler_type == "step":
        step_scheduler = torch.optim.lr_scheduler.StepLR(
            optimizer,
            step_size=args.scheduler_step,
            gamma=args.scheduler_gamma)
    else:
        step_scheduler = None

metrics_logger = logger.Logger(args)
loss_logger = logger.LossLogger(args.model_name)

for epoch in range(1, args.epochs + 1):
    print("======================== {} ========================".format(epoch))
    for param_group in optimizer.param_groups:
        print("LR: {}".format(param_group["lr"]))

    train_loader.dataset.epoch = epoch
    ################### Train #################
    all_preds, all_targs, all_masks, all_ids, train_loss, train_loss_unk = run_epoch(
        args,
        model,
        train_loader,
        optimizer,
        epoch,
        "Training",
        train=True,
        warmup_scheduler=scheduler_warmup,
    )
    train_metrics = evaluate.compute_metrics(
        args,
        all_preds,
        all_targs,
        all_masks,
        train_loss,
        train_loss_unk,
        0,
        args.train_known_labels,
    )
    loss_logger.log_losses("train.log", epoch, train_loss, train_metrics,
                           train_loss_unk)

    ################### Valid #################
    all_preds, all_targs, all_masks, all_ids, valid_loss, valid_loss_unk = run_epoch(
        args, model, valid_loader, None, epoch, "Validating")
    valid_metrics = evaluate.compute_metrics(
        args,
        all_preds,
        all_targs,
        all_masks,
        valid_loss,
        valid_loss_unk,
        0,
        args.test_known_labels,
    )
    loss_logger.log_losses("valid.log", epoch, valid_loss, valid_metrics,
                           valid_loss_unk)

    ################### Test #################
    if test_loader is not None:
        all_preds, all_targs, all_masks, all_ids, test_loss, test_loss_unk = run_epoch(
            args, model, test_loader, None, epoch, "Testing")
        test_metrics = evaluate.compute_metrics(
            args,
            all_preds,
            all_targs,
            all_masks,
            test_loss,
            test_loss_unk,
            0,
            args.test_known_labels,
        )
    else:
        test_loss, test_loss_unk, test_metrics = (
            valid_loss,
            valid_loss_unk,
            valid_metrics,
        )
    loss_logger.log_losses("test.log", epoch, test_loss, test_metrics,
                           test_loss_unk)

    if step_scheduler is not None:
        if args.scheduler_type == "step":
            step_scheduler.step(epoch)
        elif args.scheduler_type == "plateau":
            step_scheduler.step(valid_loss_unk)

    ############## Log and Save ##############
    best_valid, best_test = metrics_logger.evaluate(
        train_metrics,
        valid_metrics,
        test_metrics,
        epoch,
        0,
        model,
        valid_loss,
        test_loss,
        all_preds,
        all_targs,
        all_ids,
        args,
    )

    print(args.model_name)
